import os
import sys
import threading
from typing import List, Optional

from PySide6.QtCore import Qt, Signal, QObject, QTimer
from PySide6.QtGui import QIcon, QPalette, QColor, QFont
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QLabel,
    QPushButton, QFileDialog, QListWidget, QListWidgetItem, QStackedWidget,
    QProgressBar, QTextEdit, QCheckBox, QComboBox, QLineEdit, QFormLayout,
    QGroupBox, QRadioButton, QButtonGroup, QMessageBox, QDialog, QDialogButtonBox,
    QScrollArea, QFrame, QSpacerItem, QSizePolicy, QStatusBar, QToolButton, QGridLayout
)

# Reuse the cleaning logic from main.py
from main import (
    clean_file,
    clean_file_pandas,
    format_log_text,
    CleanStats,
    # for dedup key picker
    read_text, try_sniff_dialect, pass_one_scan, sanitize_headers
)


def set_fusion_theme(app: QApplication) -> None:
    """Modernized Fusion palette + QSS and sane font sizing."""
    app.setStyle("Fusion")
    palette = QPalette()
    palette.setColor(QPalette.Window, QColor(248, 249, 252))
    palette.setColor(QPalette.WindowText, Qt.black)
    palette.setColor(QPalette.Base, QColor(255, 255, 255))
    palette.setColor(QPalette.AlternateBase, QColor(246, 247, 250))
    palette.setColor(QPalette.ToolTipBase, Qt.white)
    palette.setColor(QPalette.ToolTipText, Qt.black)
    palette.setColor(QPalette.Text, Qt.black)
    palette.setColor(QPalette.Button, QColor(242, 244, 247))
    palette.setColor(QPalette.ButtonText, Qt.black)
    palette.setColor(QPalette.Highlight, QColor(33, 150, 243))
    palette.setColor(QPalette.HighlightedText, Qt.white)
    app.setPalette(palette)
    app.setFont(QFont("Segoe UI", 10))
    # Card-like look, responsive paddings, better inputs
    base_qss = """
        QWidget { font-size: 13px; }
        QToolTip {
            background: #111827; color: #ffffff; border: 1px solid #374151;
            padding: 6px 8px; border-radius: 6px;
        }
        QGroupBox {
            margin-top: 16px;
            padding: 12px 14px 14px 14px;
            border: 1px solid #e4e7ec;
            border-radius: 12px;
            background: #ffffff;
            box-shadow: 0px 1px 2px rgba(16,24,40,0.03);
        }
        QGroupBox::title {
            subcontrol-origin: margin;
            left: 12px; top: 6px;
            padding: 0 6px; background: transparent; font-weight: 600;
        }
        QPushButton {
            padding: 8px 16px; border-radius: 10px;
            background: #f5f7fb; border: 1px solid #e4e7ec;
        }
        QPushButton:hover { background: #eef5ff; }
        QPushButton.Primary { background: #3b82f6; color: #ffffff; border: 1px solid #3b82f6; }
        QPushButton.Primary:hover { background: #2563eb; }
        QLineEdit, QComboBox, QTextEdit, QListWidget {
            border: 1px solid #e4e7ec; border-radius: 8px; padding: 6px;
            background: #ffffff;
        }
        /* Fix white text in dropdown selections */
        QComboBox QAbstractItemView {
            selection-background-color: #e6f0ff;
            selection-color: #111827;
            outline: none;
        }
        QProgressBar { border: 1px solid #e4e7ec; border-radius: 8px; height: 18px; }
        QProgressBar::chunk { border-radius: 8px; }
        QScrollArea { border: none; background: transparent; }
        .HelpLabel { color: #6b7280; font-size: 12px; }
        /* Header bar */
        #HeaderBar {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                       stop:0 #f7f9fc, stop:1 #eef2f8);
            border: 1px solid #e4e7ec; border-radius: 12px; padding: 12px;
            box-shadow: 0px 1px 2px rgba(16,24,40,0.04);
        }
        #HeaderTitle { font-size: 18px; font-weight: 700; }
        #HeaderSubtitle { color: #57606a; }
        /* Feature chips on welcome */
        .Chip {
            border: 1px solid #e4e7ec; border-radius: 999px; padding: 6px 10px;
            background: #ffffff; color: #111827; font-size: 12px;
        }
    """
    app.setStyleSheet(base_qss)


# Friendlier welcome copy (simple language + examples)
WELCOME_TEXT = (
    "<h2>CSV Cleaner</h2>"
    "<p>Fix messy CSV/TSV files fast. No formulas, no headaches.</p>"
    "<h3>What it does</h3>"
    "<ul>"
    "<li>Makes column names clean and consistent (e.g., <code>First Name</code> → <code>first_name</code>)</li>"
    "<li>Trims extra spaces and weird characters</li>"
    "<li>Removes totally empty rows and empty-only columns</li>"
    "<li>Removes duplicate rows (or by keys you choose like <code>id</code> or <code>email</code>)</li>"
    "<li>Understands numbers, dates, and booleans and formats them neatly</li>"
    "</ul>"
    "<h3>Before you export</h3>"
    "<ul>"
    "<li>Check for typos or near-duplicates (e.g., <em>Jon</em> vs <em>John</em>)</li>"
    "<li>Make sure values make sense for your work (units, ranges, negative amounts)</li>"
    "<li>Dates can be ambiguous; pick the format you want</li>"
    "<li>Look for sensitive data before you share</li>"
    "</ul>"
)


class WorkerSignals(QObject):
    started = Signal(int, str)  # file_index, path
    step = Signal(int, str, int)  # file_index, message, percent
    finished_one = Signal(int, CleanStats, str)  # index, stats, log_text
    finished_all = Signal()
    error = Signal(str)


class CleanerWorker(threading.Thread):
    def __init__(self, files: List[str], engine: str, options: dict, outputs: List[str], signals: WorkerSignals):
        super().__init__(daemon=True)
        self.files = files
        self.engine = engine
        self.options = options
        self.outputs = outputs
        self.signals = signals

    def run(self):
        try:
            for idx, in_path in enumerate(self.files):
                out_path = self.outputs[idx]
                self.signals.started.emit(idx, in_path)

                def progress(msg: str, frac: Optional[float]):
                    pct = int((frac or 0.0) * 100)
                    self.signals.step.emit(idx, msg, pct)

                if self.engine == "pandas":
                    try:
                        stats = clean_file_pandas(
                            in_path,
                            out_path,
                            delimiter=self.options.get("delimiter"),
                            trim_cells=self.options.get("trim_cells", True),
                            drop_empty_rows=self.options.get("drop_empty_rows", True),
                            drop_duplicates=self.options.get("drop_duplicates", False),
                            dedup_keys=self.options.get("dedup_keys"),
                            remove_empty_columns=self.options.get("remove_empty_columns", False),
                            infer_types=self.options.get("infer_types", True),
                            parse_dates=self.options.get("parse_dates", True),
                            date_format=self.options.get("date_format", "%Y-%m-%d"),
                            type_threshold=self.options.get("type_threshold", 0.9),
                            fill_missing=self.options.get("fill_missing", "none"),
                            fill_constant=self.options.get("fill_constant", ""),
                            na_tokens=self.options.get("na_tokens"),
                            progress=progress,
                        )
                    except Exception as e:
                        # Fallback to streaming CSV engine on parser errors
                        self.signals.step.emit(idx, f"Pandas parsing failed: {e}. Falling back to CSV engine…", 0)
                        stats = clean_file(
                            in_path,
                            out_path,
                            delimiter=self.options.get("delimiter"),
                            trim_cells=self.options.get("trim_cells", True),
                            drop_empty_rows=self.options.get("drop_empty_rows", True),
                            drop_duplicates=self.options.get("drop_duplicates", False),
                            dedup_keys=self.options.get("dedup_keys"),
                            pad_rows=self.options.get("pad_rows", "pad"),
                            remove_empty_columns=self.options.get("remove_empty_columns", False),
                            infer_types=self.options.get("infer_types", True),
                            parse_dates=self.options.get("parse_dates", True),
                            date_format=self.options.get("date_format", "%Y-%m-%d"),
                            type_threshold=self.options.get("type_threshold", 0.9),
                            fill_missing=self.options.get("fill_missing", "none"),
                            fill_constant=self.options.get("fill_constant", ""),
                            na_tokens=self.options.get("na_tokens"),
                            progress=progress,
                        )
                else:
                    stats = clean_file(
                        in_path,
                        out_path,
                        delimiter=self.options.get("delimiter"),
                        trim_cells=self.options.get("trim_cells", True),
                        drop_empty_rows=self.options.get("drop_empty_rows", True),
                        drop_duplicates=self.options.get("drop_duplicates", False),
                        dedup_keys=self.options.get("dedup_keys"),
                        pad_rows=self.options.get("pad_rows", "pad"),
                        remove_empty_columns=self.options.get("remove_empty_columns", False),
                        infer_types=self.options.get("infer_types", True),
                        parse_dates=self.options.get("parse_dates", True),
                        date_format=self.options.get("date_format", "%Y-%m-%d"),
                        type_threshold=self.options.get("type_threshold", 0.9),
                        fill_missing=self.options.get("fill_missing", "none"),
                        fill_constant=self.options.get("fill_constant", ""),
                        na_tokens=self.options.get("na_tokens"),
                        progress=progress,
                    )

                log_text = format_log_text(stats)
                self.signals.finished_one.emit(idx, stats, log_text)

            self.signals.finished_all.emit()
        except Exception as e:
            self.signals.error.emit(str(e))


# Formal, clear welcome copy (override)
WELCOME_TEXT = (
    "<h2>CSV Cleaner</h2>"
    "<p>Clean and standardize CSV/TSV files with predictable, auditable steps.</p>"
    "<h3>Core capabilities</h3>"
    "<ul>"
    "<li>Normalize column names (e.g., <code>First Name</code> → <code>first_name</code>)</li>"
    "<li>Trim extra spaces and non-breaking spaces</li>"
    "<li>Remove fully empty rows and empty-only columns</li>"
    "<li>Remove duplicate rows or deduplicate by selected keys (e.g., <code>id</code>, <code>email</code>)</li>"
    "<li>Detect and standardize numbers, booleans, and dates</li>"
    "</ul>"
    "<h3>Review before sharing</h3>"
    "<ul>"
    "<li>Typos or near-duplicates (e.g., <em>Jon</em> and <em>John</em>)</li>"
    "<li>Business rules and ranges (units, negatives, outliers)</li>"
    "<li>Date conventions and time zones</li>"
    "<li>Sensitive or personal data</li>"
    "</ul>"
)

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("CSV Cleaner")
        self.setMinimumSize(880, 600)
        self.setStatusBar(QStatusBar(self))
        try:
            # Optional: provide an icon if available
            icon_path = os.path.join(os.path.dirname(__file__), "assets", "icon.ico")
            if os.path.exists(icon_path):
                self.setWindowIcon(QIcon(icon_path))
        except Exception:
            pass

        # Centered container with max width for nicer composition
        self.stack = QStackedWidget()
        outer = QWidget(); ov = QVBoxLayout(outer); ov.setContentsMargins(16, 12, 16, 12)
        center = QWidget(); cv = QVBoxLayout(center); cv.setContentsMargins(0, 0, 0, 0)
        center.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Expanding)
        center.setMaximumWidth(1400)  # plenty of space so columns never overlap
        # Header bar
        header = QFrame(); header.setObjectName("HeaderBar")
        hv = QVBoxLayout(header); hv.setContentsMargins(12, 10, 12, 10)
        htitle = QLabel("CSV Cleaner"); htitle.setObjectName("HeaderTitle")
        top_row = QHBoxLayout(); top_row.addWidget(htitle); top_row.addStretch(1)
        hsubtitle = QLabel("Tidy data, consistent types, and reproducible logs."); hsubtitle.setObjectName("HeaderSubtitle")
        hv.addLayout(top_row); hv.addWidget(hsubtitle)
        cv.addWidget(header)
        cv.addSpacing(8)
        cv.addWidget(self.stack)
        ov.addWidget(center, 1, Qt.AlignHCenter)
        self.setCentralWidget(outer)
        self._compact = False  # density flag
        self._info_icon_path = None  # no external image

        self.files: List[str] = []
        self.outputs: List[str] = []
        self.logs_per_file: List[str] = []

        self._build_pages()

    # Pages
    def _build_pages(self):
        self.page_welcome = self._build_welcome_page2()
        self.page_select = self._build_select_page()
        self.page_progress = self._build_progress_page()
        self.page_result = self._build_result_page()
        self.stack.addWidget(self.page_welcome)
        self.stack.addWidget(self.page_select)
        self.stack.addWidget(self.page_progress)
        self.stack.addWidget(self.page_result)
        self.stack.setCurrentWidget(self.page_welcome)
        # Status nudge so users notice tooltips
        self.statusBar().showMessage("Tip: hover options for quick explanations.", 3000)

    def _build_welcome_page(self) -> QWidget:
        w = QWidget()
        v = QVBoxLayout(w); v.setContentsMargins(8, 8, 8, 8)
        card = QFrame(); card.setObjectName("HeaderBar")
        cv = QVBoxLayout(card); cv.setContentsMargins(18,16,18,16)
        t1 = QLabel("<b>CSV Cleaner</b>"); t1.setObjectName("HeaderTitle")
        t2 = QLabel("Clean and standardize CSV/TSV files with predictable, auditable steps.")
        t2.setObjectName("HeaderSubtitle")
        bullets = QLabel(
            "<ul>"
            "<li>Consistent column names (e.g., <code>First Name</code> → <code>first_name</code>)</li>"
            "<li>Removes empty rows/columns and whole-row duplicates</li>"
            "<li>Understands numbers, booleans, and dates</li>"
            "</ul>"
        )
        bullets.setWordWrap(True)
        cv.addWidget(t1); cv.addWidget(t2); cv.addSpacing(6); cv.addWidget(bullets)
        v.addWidget(card, 0, Qt.AlignHCenter)
        v.addSpacing(12)
        btn = QPushButton("Get started"); btn.setProperty("class","Primary")
        btn.clicked.connect(lambda: self.stack.setCurrentWidget(self.page_select))
        v.addWidget(btn, alignment=Qt.AlignRight)
        return w

    def _build_welcome_page2(self) -> QWidget:
        w = QWidget()
        v = QVBoxLayout(w); v.setContentsMargins(8, 8, 8, 8)
        card = QFrame(); card.setObjectName("HeaderBar")
        cv = QVBoxLayout(card); cv.setContentsMargins(18,16,18,16)
        t1 = QLabel("<b>CSV Cleaner</b>"); t1.setObjectName("HeaderTitle")
        t2 = QLabel("Clean and standardize CSV/TSV files with predictable, auditable steps.")
        t2.setObjectName("HeaderSubtitle")
        chips = QHBoxLayout(); chips.setSpacing(8)
        for txt in ["Fast cleaning", "Type-aware", "Clear logs"]:
            c = QLabel(txt); c.setProperty("class","Chip")
            chips.addWidget(c)
        chips.addStretch(1)
        cv.addWidget(t1); cv.addWidget(t2); cv.addSpacing(6); cv.addLayout(chips)
        v.addWidget(card, 0, Qt.AlignHCenter)
        v.addSpacing(10)

        grid_card = QFrame(); grid_card.setObjectName("HeaderBar")
        gv_outer = QVBoxLayout(grid_card); gv_outer.setContentsMargins(16,14,16,14); gv_outer.setSpacing(12)
        grid = QGridLayout(); grid.setHorizontalSpacing(32); grid.setVerticalSpacing(6)
        gv_outer.addLayout(grid)

        left = QGroupBox("What it does")
        right = QGroupBox("Review before sharing")
        for gb in (left, right):
            gb.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)

        left_lbl = QLabel(
            "• Makes column names consistent (e.g., <code>First Name</code> → <code>first_name</code>)<br>"
            "• Trims extra spaces and non-breaking spaces<br>"
            "• Removes fully empty rows and empty-only columns<br>"
            "• Removes whole-row duplicates or deduplicates by keys (e.g., <code>id</code>, <code>email</code>)<br>"
            "• Detects and standardizes numbers, booleans, and dates"
        ); left_lbl.setWordWrap(True)
        rl = QVBoxLayout(left); rl.addWidget(left_lbl)

        right_lbl = QLabel(
            "• Typos or near-duplicates (e.g., <i>Jon</i> and <i>John</i>)<br>"
            "• Business rules and ranges (units, negatives, outliers)<br>"
            "• Date conventions and time zones<br>"
            "• Sensitive or personal data"
        ); right_lbl.setWordWrap(True)
        rr = QVBoxLayout(right); rr.addWidget(right_lbl)

        left.setMinimumWidth(420)
        right.setMinimumWidth(420)
        grid.addWidget(left, 0, 0)
        grid.addWidget(right, 0, 1)
        grid.setColumnStretch(0, 1)
        grid.setColumnStretch(1, 1)
        v.addWidget(grid_card, 0, Qt.AlignHCenter)

        qs_row = QHBoxLayout(); qs_row.setSpacing(10)
        btn_safe = QPushButton("Safe defaults")
        btn_aggr = QPushButton("Aggressive cleanup")
        btn_large = QPushButton("Large file mode")
        for b in (btn_safe, btn_aggr, btn_large):
            b.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        qs_row.addStretch(1); qs_row.addWidget(btn_safe); qs_row.addWidget(btn_aggr); qs_row.addWidget(btn_large); qs_row.addStretch(1)
        v.addLayout(qs_row)

        btn_safe.clicked.connect(lambda: self._apply_quick_start("safe"))
        btn_aggr.clicked.connect(lambda: self._apply_quick_start("aggressive"))
        btn_large.clicked.connect(lambda: self._apply_quick_start("large"))

        btn = QPushButton("Get started"); btn.setProperty("class","Primary")
        btn.clicked.connect(lambda: self.stack.setCurrentWidget(self.page_select))
        v.addWidget(btn, alignment=Qt.AlignRight)
        return w

    def _build_select_page(self) -> QWidget:
        # Wrap the whole page in a scroll area so smaller windows still show everything
        container = QWidget(); v = QVBoxLayout(container); v.setContentsMargins(4, 4, 4, 8)

        # File picker + list
        files_box = QGroupBox("Files to clean")
        vb = QVBoxLayout(files_box)
        self.list_files = QListWidget()
        self.list_files.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        hb = QHBoxLayout()
        btn_add = QPushButton("Add files…")
        btn_add.clicked.connect(self._pick_files)
        btn_clear = QPushButton("Clear list")
        btn_clear.clicked.connect(self._clear_files)
        hb.addWidget(btn_add)
        hb.addWidget(btn_clear)
        hb.addStretch(1)
        vb.addWidget(self.list_files)
        vb.addLayout(hb)
        v.addWidget(files_box)

        # Options (single full-width column; reliable and readable)
        opts_box = QGroupBox("Options")
        form = QFormLayout(opts_box)
        form.setLabelAlignment(Qt.AlignLeft)
        form.setFormAlignment(Qt.AlignTop)
        form.setHorizontalSpacing(14)
        form.setVerticalSpacing(10)
        # Grow fields to fill available width
        form.setFieldGrowthPolicy(QFormLayout.ExpandingFieldsGrow)
        self.combo_engine = QComboBox(); self.combo_engine.addItems(["pandas", "csv"])
        self.combo_engine.setCurrentText("pandas")
        engine_help = QLabel("Pandas is faster and richer for most files. CSV engine uses less memory and tolerates odd formatting.")
        engine_help.setObjectName("HelpLabel"); engine_help.setProperty("class", "HelpLabel"); engine_help.setWordWrap(True)
        engine_info = QToolButton(); engine_info.setText("Info"); engine_info.clicked.connect(self._show_engine_info)
        self.chk_trim = QCheckBox("Trim cells and headers"); self.chk_trim.setChecked(True)
        self.chk_trim.setToolTip("<p style='width:280px'>Remove extra spaces and non-breaking spaces in all cells and headers.</p>")
        trim_help = QLabel("Removes stray spaces so matching and types are consistent.")
        trim_help.setProperty("class","HelpLabel"); trim_help.setWordWrap(True)
        self.chk_drop_empty_rows = QCheckBox("Drop fully-empty rows"); self.chk_drop_empty_rows.setChecked(True)
        self.chk_drop_empty_rows.setToolTip("<p style='width:280px'>Delete rows where every cell is blank.</p>")
        empty_rows_help = QLabel("Safely removes records that contain no data at all.")
        empty_rows_help.setProperty("class","HelpLabel"); empty_rows_help.setWordWrap(True)
        self.chk_remove_empty_cols = QCheckBox("Remove empty columns (all rows empty)")
        self.chk_remove_empty_cols.setToolTip("<p style='width:280px'>Delete columns that contain no values in any row.</p>")
        empty_cols_help = QLabel("Hides useless columns that are empty in every row.")
        empty_cols_help.setProperty("class","HelpLabel"); empty_cols_help.setWordWrap(True)
        self.chk_drop_dupes = QCheckBox("Remove duplicate rows")
        self.chk_drop_dupes.setToolTip("<p style='width:280px'>Remove repeated rows. For targeted deduplication, choose keys such as <code>id</code> or <code>email</code>.</p>")
        dupes_help = QLabel("Whole-row duplicates are removed. For smarter control, set Dedup keys.")
        dupes_help.setProperty("class","HelpLabel"); dupes_help.setWordWrap(True)
        dedup_row = QHBoxLayout()
        self.edit_dedup_keys = QLineEdit(); self.edit_dedup_keys.setPlaceholderText("e.g. id,email (optional)")
        self.edit_dedup_keys.setToolTip("<p style='width:280px'>Columns that should be unique together. Examples: <code>id</code> or <code>id,email</code>.</p>")
        self.btn_pick_keys = QPushButton("Pick…")
        self.btn_pick_keys.setToolTip("Open a list of columns from your first file and select dedup keys.")
        self.btn_pick_keys.clicked.connect(self._open_pick_keys_dialog)
        dedup_info = QToolButton(); dedup_info.setText("Info"); dedup_info.clicked.connect(self._show_dedup_info)
        dedup_row.addWidget(self.edit_dedup_keys)
        dedup_row.addWidget(self.btn_pick_keys)
        dedup_row.addWidget(dedup_info)
        dedup_help = QLabel("Use this when you want unique rows by ID or by a combination like id+email.")
        dedup_help.setProperty("class","HelpLabel"); dedup_help.setWordWrap(True)
        self.chk_infer = QCheckBox("Infer numbers/booleans and parse dates"); self.chk_infer.setChecked(True)
        self.chk_infer.setToolTip("Auto-detect numeric/boolean columns and standardize date formats.")
        infer_help = QLabel("On by default. Turn off if your file has tricky values that look like numbers but aren’t (e.g., zip codes).")
        infer_help.setProperty("class","HelpLabel"); infer_help.setWordWrap(True)
        self.combo_fill = QComboBox(); self.combo_fill.addItems(["none", "empty", "constant", "zero", "mean", "median", "mode"])
        self.combo_fill.setToolTip("How to fill missing values. 'none' leaves blanks. 'mean/median' work for numbers.")
        fill_help = QLabel("Leaving blanks is safest. Averages and medians change your data; use for analysis-only copies.")
        fill_help.setProperty("class","HelpLabel"); fill_help.setWordWrap(True)
        self.edit_fill_const = QLineEdit(); self.edit_fill_const.setPlaceholderText("fill value when constant")
        self.edit_fill_const.setToolTip("Used only when Fill missing = constant.")
        const_help = QLabel("Only used if you choose constant. Example: fill missing city with “Unknown”.")
        const_help.setProperty("class","HelpLabel"); const_help.setWordWrap(True)
        self.edit_date_fmt = QLineEdit("%Y-%m-%d")
        self.edit_date_fmt.setToolTip("Output date format, e.g. %Y-%m-%d → 2024-03-01")
        datefmt_help = QLabel("ISO-like formats are safest for other tools. Example shown is year-month-day.")
        datefmt_help.setProperty("class","HelpLabel"); datefmt_help.setWordWrap(True)

        for wdg in [self.combo_engine, self.edit_dedup_keys, self.combo_fill, self.edit_fill_const, self.edit_date_fmt]:
            wdg.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        # Engine row with Info + helper
        eng_row = QHBoxLayout(); eng_row.addWidget(self.combo_engine); eng_row.addWidget(engine_info)
        form.addRow("Engine:", QWidget()); form.itemAt(form.rowCount()-1, QFormLayout.FieldRole).widget().setLayout(eng_row)
        form.addRow("", engine_help)
        # Core toggles with helpers
        form.addRow("", self.chk_trim);            form.addRow("", trim_help)
        form.addRow("", self.chk_drop_empty_rows); form.addRow("", empty_rows_help)
        form.addRow("", self.chk_remove_empty_cols); form.addRow("", empty_cols_help)
        # Duplicates + dedup keys
        form.addRow("", self.chk_drop_dupes);      form.addRow("", dupes_help)
        form.addRow("Dedup keys:", QWidget()); form.itemAt(form.rowCount()-1, QFormLayout.FieldRole).widget().setLayout(dedup_row)
        form.addRow("", dedup_help)
        # Typing/dates
        form.addRow("Typing & dates:", self.chk_infer); form.addRow("", infer_help)
        # Fill strategy
        form.addRow("Fill missing:", self.combo_fill);  form.addRow("", fill_help)
        form.addRow("Fill constant:", self.edit_fill_const); form.addRow("", const_help)
        form.addRow("Date format:", self.edit_date_fmt); form.addRow("", datefmt_help)
        # Density
        self.chk_compact = QCheckBox("Compact layout")
        self.chk_compact.setToolTip("<p style='width:280px'>Reduce padding and spacing for smaller screens.</p>")
        self.chk_compact.stateChanged.connect(self._toggle_density)
        compact_help = QLabel("Reduces padding for small screens. Most users can leave this off.")
        compact_help.setProperty("class","HelpLabel"); compact_help.setWordWrap(True)
        form.addRow("", self.chk_compact); form.addRow("", compact_help)
        v.addWidget(opts_box)

        # Output destination
        out_box = QGroupBox("Output destination")
        ob = QVBoxLayout(out_box)
        self.rad_overwrite = QRadioButton("Overwrite original files")
        self.rad_folder = QRadioButton("Save to this folder:")
        self.rad_folder.setChecked(True)
        row = QHBoxLayout()
        self.lbl_out_dir = QLabel(os.getcwd())
        btn_browse_out = QPushButton("Browse…")
        btn_browse_out.clicked.connect(self._pick_output_folder)
        row.addWidget(self.lbl_out_dir)
        row.addWidget(btn_browse_out)
        ob.addWidget(self.rad_overwrite)
        ob.addWidget(self.rad_folder)
        ob.addLayout(row)
        v.addWidget(out_box)

        # Next
        btn_next = QPushButton("Start cleaning")
        btn_next.setProperty("class","Primary")
        btn_next.clicked.connect(self._start_cleaning)
        v.addWidget(btn_next, alignment=Qt.AlignRight)

        # Put content into a scroll area (for non-maximized windows)
        area = QScrollArea(); area.setWidgetResizable(True); area.setWidget(container)
        area.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)  # forbid bottom scrollbar
        return area

    # ---------- Quick start presets ----------
    def _apply_quick_start(self, mode: str):
        """
        Apply preset options and switch to Options page.
        Safe: pandas, trim ON, drop empty rows ON, infer ON, no dedup, no remove empty cols, fill none.
        Aggressive: pandas, trim ON, drop empty rows ON, remove empty cols ON, dedup by id if present, infer ON, fill empty.
        Large: csv engine, trim ON, drop empty rows ON, infer OFF, no fill, no dedup.
        """
        def set_controls():
            # Bail out if controls are not yet built (shouldn't happen once select page exists)
            if not hasattr(self, "combo_engine"):
                return
            if mode == "safe":
                self.combo_engine.setCurrentText("pandas")
                self.chk_trim.setChecked(True)
                self.chk_drop_empty_rows.setChecked(True)
                self.chk_remove_empty_cols.setChecked(False)
                self.chk_drop_dupes.setChecked(False)
                self.edit_dedup_keys.setText("")
                self.chk_infer.setChecked(True)
                self.combo_fill.setCurrentText("none")
            elif mode == "aggressive":
                self.combo_engine.setCurrentText("pandas")
                self.chk_trim.setChecked(True)
                self.chk_drop_empty_rows.setChecked(True)
                self.chk_remove_empty_cols.setChecked(True)
                self.chk_drop_dupes.setChecked(True)
                # Suggest id if it exists; user can change in picker
                self.edit_dedup_keys.setText("id")
                self.chk_infer.setChecked(True)
                self.combo_fill.setCurrentText("empty")
            else:  # large
                self.combo_engine.setCurrentText("csv")
                self.chk_trim.setChecked(True)
                self.chk_drop_empty_rows.setChecked(True)
                self.chk_remove_empty_cols.setChecked(False)
                self.chk_drop_dupes.setChecked(False)
                self.edit_dedup_keys.setText("")
                self.chk_infer.setChecked(False)
                self.combo_fill.setCurrentText("none")

        # Navigate, then apply (controls live on the Options page)
        self.stack.setCurrentWidget(self.page_select)
        # Ensure the page is visible before we touch widgets
        try:
            from PySide6.QtCore import QTimer
            QTimer.singleShot(0, set_controls)
        except Exception:
            set_controls()

    def _build_progress_page(self) -> QWidget:
        w = QWidget()
        v = QVBoxLayout(w)
        self.lbl_current = QLabel("Ready")
        self.progress = QProgressBar()
        self.progress.setRange(0, 100)
        self.list_steps = QListWidget()
        v.addWidget(self.lbl_current)
        v.addWidget(self.progress)
        v.addWidget(QLabel("Live steps:"))
        v.addWidget(self.list_steps)
        return w

    def _build_result_page(self) -> QWidget:
        w = QWidget()
        v = QVBoxLayout(w)
        self.lbl_summary = QLabel("")
        self.txt_log = QTextEdit(); self.txt_log.setReadOnly(True)
        btns = QHBoxLayout()
        self.btn_open_folder = QPushButton("Open output folder")
        self.btn_open_folder.clicked.connect(self._open_output_folder)
        self.btn_done = QPushButton("Done")
        self.btn_done.clicked.connect(self.close)
        btns.addWidget(self.btn_open_folder)
        btns.addStretch(1)
        btns.addWidget(self.btn_done)
        v.addWidget(self.lbl_summary)
        v.addWidget(QLabel("Log:"))
        v.addWidget(self.txt_log)
        v.addLayout(btns)
        return w

    # File pickers
    def _pick_files(self):
        files, _ = QFileDialog.getOpenFileNames(self, "Select CSV/TSV files", os.getcwd(),
                                                "CSV/TSV (*.csv *.tsv);;All files (*.*)")
        for f in files:
            if f not in self.files:
                self.files.append(f)
                self.list_files.addItem(QListWidgetItem(f))

    def _clear_files(self):
        self.files.clear()
        self.list_files.clear()

    def _pick_output_folder(self):
        folder = QFileDialog.getExistingDirectory(self, "Select output folder", self.lbl_out_dir.text())
        if folder:
            self.lbl_out_dir.setText(folder)

    # Start
    def _start_cleaning(self):
        if not self.files:
            QMessageBox.warning(self, "No files", "Please add at least one CSV/TSV file.")
            return
        engine = self.combo_engine.currentText()
        infer = self.chk_infer.isChecked()
        opts = dict(
            delimiter=None,
            trim_cells=self.chk_trim.isChecked(),
            drop_empty_rows=self.chk_drop_empty_rows.isChecked(),
            drop_duplicates=self.chk_drop_dupes.isChecked(),
            dedup_keys=[s.strip() for s in self.edit_dedup_keys.text().split(',')] if self.edit_dedup_keys.text().strip() else None,
            pad_rows="pad",
            remove_empty_columns=self.chk_remove_empty_cols.isChecked(),
            infer_types=infer,
            parse_dates=infer,
            date_format=self.edit_date_fmt.text() or "%Y-%m-%d",
            type_threshold=0.9,
            fill_missing=self.combo_fill.currentText(),
            fill_constant=self.edit_fill_const.text(),
            na_tokens=None,
        )

        # Compute outputs
        self.outputs = []
        if self.rad_overwrite.isChecked():
            self.outputs = list(self.files)
        else:
            out_dir = self.lbl_out_dir.text()
            if not out_dir:
                QMessageBox.warning(self, "No folder", "Please choose an output folder.")
                return
            os.makedirs(out_dir, exist_ok=True)
            for f in self.files:
                base = os.path.basename(f)
                stem, ext = os.path.splitext(base)
                out = os.path.join(out_dir, f"{stem}_cleaned{ext or '.csv'}")
                self.outputs.append(out)

        # Reset progress view
        self.list_steps.clear()
        self.lbl_current.setText("Starting…")
        self.progress.setValue(0)
        self.logs_per_file = [""] * len(self.files)
        self.stack.setCurrentWidget(self.page_progress)

        # Run in background
        self.signals = WorkerSignals()
        self.signals.started.connect(self._on_started)
        self.signals.step.connect(self._on_step)
        self.signals.finished_one.connect(self._on_finished_one)
        self.signals.finished_all.connect(self._on_finished_all)
        self.signals.error.connect(self._on_error)

        self.worker = CleanerWorker(self.files, engine, opts, self.outputs, self.signals)
        self.worker.start()

    # Worker slots
    def _on_started(self, idx: int, path: str):
        self.list_steps.addItem(QListWidgetItem(f"File {idx+1}/{len(self.files)}: {path}"))
        self.lbl_current.setText(os.path.basename(path))

    def _on_step(self, idx: int, message: str, percent: int):
        if message:
            self.list_steps.addItem(QListWidgetItem(f"✓ {message}"))
            self.list_steps.scrollToBottom()
        if percent >= 0:
            self.progress.setValue(percent)

    def _on_finished_one(self, idx: int, stats: CleanStats, log_text: str):
        self.logs_per_file[idx] = log_text
        self.list_steps.addItem(QListWidgetItem(f"✔ Completed file {idx+1}/{len(self.files)} -> {self.outputs[idx]}"))
        self.list_steps.scrollToBottom()
        self.progress.setValue(100)

    def _on_finished_all(self):
        # Show results
        combined = []
        for i, (src, dst) in enumerate(zip(self.files, self.outputs)):
            combined.append(f"Input: {src}\nOutput: {dst}\n" + (self.logs_per_file[i] or ""))
            combined.append("\n" + "=" * 60 + "\n")
        self.txt_log.setPlainText("\n".join(combined))
        self.lbl_summary.setText(f"Cleaned {len(self.files)} file(s).")
        self.stack.setCurrentWidget(self.page_result)

    def _on_error(self, msg: str):
        QMessageBox.critical(self, "Error", msg)
        self.stack.setCurrentWidget(self.page_select)

    def _open_output_folder(self):
        if self.outputs:
            folder = os.path.dirname(self.outputs[0]) if len(set(os.path.dirname(x) for x in self.outputs)) == 1 else os.path.dirname(self.outputs[0])
            try:
                if sys.platform.startswith("win"):
                    os.startfile(folder)  # type: ignore
                elif sys.platform == "darwin":
                    os.system(f"open '{folder}'")
                else:
                    os.system(f"xdg-open '{folder}'")
            except Exception:
                pass

    # ---------- Info dialogs ----------
    def _show_engine_info(self):
        if not self.files:
            msg = (
                "<b>Pandas engine</b>: faster operations and better type handling.<br>"
                "<b>CSV engine</b>: lower memory use, more tolerant of odd quoting.<br><br>"
                "Rule of thumb: use Pandas for most files. Use CSV for extremely large files "
                "or when Pandas fails to parse."
            )
            QMessageBox.information(self, "Engine guidance", msg)
            return
        # Light recommendation from first file
        try:
            sample, enc = read_text(self.files[0])
            dialect = try_sniff_dialect(sample, None)
            header_in, _ = pass_one_scan(self.files[0], dialect, enc)
            # quick row estimate (cheap line count)
            with open(self.files[0], "r", encoding=enc, errors="replace") as f:
                approx_rows = sum(1 for _ in f) - 1
            cols = len(header_in) if header_in else 10
            size_hint = approx_rows * max(cols, 1)
            if approx_rows < 0:
                approx_rows = 0
            rec = "Pandas (recommended)" if size_hint < 2_000_000 else "CSV (recommended for very large files)"
            msg = (
                f"Detected ~{approx_rows:,} rows and {cols} columns.<br><br>"
                "<b>Pandas</b>: best for most datasets; richer typing and speed.<br>"
                "<b>CSV</b>: safer for extreme sizes or odd quoting; uses less memory.<br><br>"
                f"Suggested: <b>{rec}</b>."
            )
            QMessageBox.information(self, "Engine guidance", msg)
        except Exception:
            QMessageBox.information(
                self,
                "Engine guidance",
                "Pandas: faster, richer types. CSV: lower memory, more tolerant.<br>"
                "If parsing fails with Pandas, switch to CSV.",
            )

    def _show_dedup_info(self):
        if not self.files:
            QMessageBox.information(
                self,
                "Deduplication",
                "Dedup keys define which columns must be unique together.\n"
                "Examples: id or id,email. Use when duplicates share the same identifier "
                "even if other columns differ.",
            )
            return
        try:
            sample, enc = read_text(self.files[0])
            dialect = try_sniff_dialect(sample, None)
            header_in, _ = pass_one_scan(self.files[0], dialect, enc)
            cols = sanitize_headers(header_in) if header_in else []
            # Estimate uniqueness for each column from the first ~500 rows
            uniq = {}
            import csv as _csv
            with open(self.files[0], "r", encoding=enc, errors="replace", newline="") as f:
                r = _csv.reader(f, dialect)
                next(r, None)
                seen = {i: set() for i in range(len(cols))}
                total = 0
                for row in r:
                    total += 1
                    for i, c in enumerate(cols):
                        if i < len(row):
                            seen[i].add(row[i])
                    if total >= 500:
                        break
                for i, c in enumerate(cols):
                    uniq[c] = (len(seen[i]) / max(total, 1)) if total else 0.0
            suggestions = [c for c, u in sorted(uniq.items(), key=lambda x: -x[1]) if u > 0.9][:3]
            sugg_txt = ", ".join(suggestions) if suggestions else "None detected"
            msg = (
                "Dedup keys define uniqueness. Choose a single ID column or a combination like id+email.<br>"
                f"<br><b>Good candidates (sample-based):</b> {sugg_txt}<br>"
                "Pick keys that identify a real-world entity once."
            )
            QMessageBox.information(self, "Deduplication guidance", msg)
        except Exception:
            QMessageBox.information(
                self,
                "Deduplication",
                "Choose columns that together identify one record. Example: id or id,email.",
            )

    # Density toggle handler
    def _toggle_density(self, state: int):
        compact = state == Qt.Checked
        if compact == self._compact:
            return
        self._compact = compact
        # Apply compact variant of the stylesheet by shrinking paddings
        base = self.window().styleSheet()
        compact_addon = """
            QGroupBox { padding: 8px 10px 10px 10px; }
            QPushButton { padding: 5px 10px; }
            QLineEdit, QComboBox, QTextEdit, QListWidget { padding: 4px; }
        """
        if compact:
            self.window().setStyleSheet(base + compact_addon)
            self.statusBar().showMessage("Compact layout enabled", 2000)
        else:
            # Re-apply base theme
            set_fusion_theme(QApplication.instance())
            self.statusBar().showMessage("Compact layout disabled", 2000)

    # -------- Dedup key picker ----------
    def _open_pick_keys_dialog(self):
        if not self.files:
            QMessageBox.information(self, "No file", "Add at least one file first.")
            return
        # Read header from the first file and sanitize like the cleaner will
        try:
            sample, enc = read_text(self.files[0])
            dialect = try_sniff_dialect(sample, None)
            header_in, _ = pass_one_scan(self.files[0], dialect, enc)
            if not header_in:
                QMessageBox.warning(self, "No header", "Could not read column names from the first file.")
                return
            cols = sanitize_headers(header_in)
        except Exception as e:
            QMessageBox.critical(self, "Error reading columns", str(e))
            return

        dlg = QDialog(self)
        dlg.setWindowTitle("Pick columns for Dedup Keys")
        layout = QVBoxLayout(dlg)
        info = QLabel("Choose columns that should be unique together (e.g., id or id + email).")
        info.setWordWrap(True)
        layout.addWidget(info)

        # scrollable list of toggle buttons
        area = QScrollArea(); area.setWidgetResizable(True)
        inner = QWidget(); iv = QVBoxLayout(inner)
        checks = []
        for c in cols:
            cb = QCheckBox(c)
            iv.addWidget(cb); checks.append(cb)
        iv.addStretch(1)
        area.setWidget(inner)
        layout.addWidget(area)

        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        layout.addWidget(btns)
        btns.accepted.connect(dlg.accept)
        btns.rejected.connect(dlg.reject)

        if dlg.exec():
            picked = [cb.text() for cb in checks if cb.isChecked()]
            self.edit_dedup_keys.setText(",".join(picked))


def main():
    app = QApplication(sys.argv)
    set_fusion_theme(app)
    win = MainWindow()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
