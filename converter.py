import sys
import os
import html
from decimal import Decimal, InvalidOperation
import pandas as pd
import pandavro as pdx
import pyarrow as pa
import pyarrow.parquet as pq
from PyQt5.QtWidgets import (QApplication, QWidget, QVBoxLayout, QHBoxLayout, QLabel, 
                             QTextEdit, QPushButton, QFrame, QFileDialog, QComboBox)
from PyQt5.QtCore import Qt, QThread, pyqtSignal, QTimer

# ==========================================
# 1. BACKGROUND WORKER THREAD
# ==========================================
class ConverterWorker(QThread):
    # Signals to communicate with the main GUI thread
    update_status = pyqtSignal(str)
    finished = pyqtSignal()

    GERMAN_CHAR_MAP = {
        'ä': 'ae',
        'ö': 'oe',
        'ü': 'ue',
        'Ä': 'ae',
        'Ö': 'oe',
        'Ü': 'ue',
        'ß': 'ss',
    }

    def __init__(self, file_paths, output_dir, output_format, use_german_dict=False):
        super().__init__()
        self.file_paths = file_paths
        self.output_dir = output_dir 
        self.output_format = output_format 
        self.use_german_dict = use_german_dict
        self.integer_headers = set()
        self.bignumeric_headers = set()

    @classmethod
    def _replace_german_chars(cls, value):
        text = str(value)
        return ''.join(cls.GERMAN_CHAR_MAP.get(ch, ch) for ch in text)

    @classmethod
    def _normalize_header(cls, header):
        cleaned = cls._replace_german_chars(header).strip().lower()
        cleaned = pd.Series([cleaned]).str.replace(r'[^a-zA-Z0-9_]', '_', regex=True).iloc[0]
        cleaned = pd.Series([cleaned]).str.replace(r'_+', '_', regex=True).str.strip('_').iloc[0]
        if cleaned and cleaned[0].isdigit():
            cleaned = '_' + cleaned
        return cleaned or 'column'

    @classmethod
    def _sanitize_headers(cls, columns):
        seen = {}
        sanitized = []
        for col in columns:
            base = cls._normalize_header(col)
            count = seen.get(base, 0)
            if count == 0:
                candidate = base
            else:
                candidate = f"{base}_{count + 1}"
            seen[base] = count + 1
            sanitized.append(candidate)
        return sanitized

    @staticmethod
    def _to_decimal(value):
        if value is None or pd.isna(value):
            return None
        if isinstance(value, Decimal):
            return value
        text = str(value).strip().replace(',', '')
        if not text:
            return None
        try:
            return Decimal(text)
        except (InvalidOperation, ValueError):
            return None

    @staticmethod
    def _decimal_profile(value):
        as_decimal = ConverterWorker._to_decimal(value)
        if as_decimal is None:
            return None

        normalized = as_decimal.normalize()
        digits = normalized.as_tuple().digits
        exponent = normalized.as_tuple().exponent
        precision = len(digits)
        scale = -exponent if exponent < 0 else 0
        return as_decimal, precision, scale

    def _infer_numeric_targets(self, df):
        inferred_integer = set()
        inferred_bignumeric = set()

        for col in df.columns:
            col_name = str(col)
            non_null = df[col].dropna()
            if non_null.empty:
                continue

            profiles = []
            parseable_count = 0
            for raw in non_null:
                profile = self._decimal_profile(raw)
                if profile is None:
                    continue
                parseable_count += 1
                profiles.append(profile)

            coverage = parseable_count / len(non_null)
            if coverage < 0.95 or not profiles:
                continue

            max_precision = max(p[1] for p in profiles)
            max_scale = max(p[2] for p in profiles)

            if max_scale == 0 and max_precision <= 38:
                inferred_integer.add(col_name)
            elif max_precision <= 76 and max_scale <= 38:
                inferred_bignumeric.add(col_name)
            else:
                self.update_status.emit(
                    f"Warning: Header '{col_name}' exceeds BIGNUMERIC limits (precision={max_precision}, scale={max_scale})"
                )

        self.integer_headers = inferred_integer
        self.bignumeric_headers = inferred_bignumeric

    def _apply_decimal_columns(self, df):
        for col in sorted(self.integer_headers):
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

        for col in sorted(self.bignumeric_headers):
            df[col] = df[col].apply(self._to_decimal)

    def _apply_german_dictionary_headers(self, columns):
        return [self._replace_german_chars(col) for col in columns]

    def _apply_german_dictionary_records(self, df):
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].apply(
                lambda v: self._replace_german_chars(v) if v is not None else None
            )

    @staticmethod
    def _normalize_nulls_for_parquet(df):
        # Ensure NaN/NaT become Python None across all dtypes for parquet export.
        return df.astype(object).where(pd.notna(df), None)

    @staticmethod
    def _normalize_nulls_for_avro(df):
        # Keep stable pandas dtypes for avro inference while preserving null semantics.
        avro_df = df.copy()
        for col in avro_df.columns:
            series = avro_df[col]
            if series.isna().all():
                avro_df[col] = pd.Series([None] * len(avro_df), dtype='string')
            elif pd.api.types.is_integer_dtype(series):
                avro_df[col] = series.astype('Int64')
            elif pd.api.types.is_float_dtype(series):
                avro_df[col] = series.astype('Float64')
            elif pd.api.types.is_bool_dtype(series):
                avro_df[col] = series.astype('boolean')
            else:
                avro_df[col] = series.astype('string')
        return avro_df

    def _write_parquet(self, out_path, df):
        arrays = []
        for col in df.columns:
            values = df[col].tolist()
            if col in self.bignumeric_headers:
                arrays.append(pa.array(values, type=pa.decimal256(76, 38)))
            elif col in self.integer_headers:
                arrays.append(pa.array(values, type=pa.int64()))
            else:
                arrays.append(pa.array(values))

        table = pa.Table.from_arrays(arrays, names=df.columns.tolist())
        pq.write_table(table, out_path)

    def run(self):
        for file in self.file_paths:
            try:
                self.update_status.emit(f"Processing: {os.path.basename(file)}...")
                
                # Read the input file based on its extension
                ext = os.path.splitext(file)[1].lower()
                if ext == '.csv':
                    df = pd.read_csv(file)
                elif ext in ['.xls', '.xlsx']:
                    df = pd.read_excel(file)
                else:
                    self.update_status.emit(f"Skipped {os.path.basename(file)} (Unsupported format)")
                    continue
                
                # Normalize headers to lowercase and valid identifier format.
                df.columns = self._sanitize_headers(df.columns)

                if self.use_german_dict:
                    df.columns = self._apply_german_dictionary_headers(df.columns)

                # Keep column names as strings to avoid mixed-type sort/comparison errors.
                df.columns = [str(col) for col in df.columns]

                if self.use_german_dict:
                    self._apply_german_dictionary_records(df)

                # Extract the base file name without the extension
                base_name = os.path.splitext(os.path.basename(file))[0]
                
                # Construct the new file path based on the selected format
                out_path = os.path.join(self.output_dir, base_name + self.output_format)
                
                # Convert and save dataframe to the requested format
                if self.output_format == '.avro':
                    avro_df = self._normalize_nulls_for_avro(df)
                    pdx.to_avro(out_path, avro_df)
                elif self.output_format == '.parquet':
                    # Detect numeric targets only for parquet typing.
                    self._infer_numeric_targets(df)
                    self.update_status.emit(
                        f"Auto-detected BIGNUMERIC headers: {', '.join(sorted(self.bignumeric_headers, key=str)) or 'none'}"
                    )

                    # Apply decimal typing only for parquet to keep avro conversion stable.
                    df = self._normalize_nulls_for_parquet(df)
                    self._apply_decimal_columns(df)
                    self._write_parquet(out_path, df)
                
                self.update_status.emit(f"Success: Saved {os.path.basename(out_path)}")
            except Exception as e:
                self.update_status.emit(f"Error at {os.path.basename(file)}: {str(e)}")
        
        # Emit finished signal when the loop is done
        self.finished.emit()

# ==========================================
# 2. MAIN GUI APPLICATION
# ==========================================
class DataConverterApp(QWidget):
    def __init__(self):
        super().__init__()
        self.file_list = []
        self.output_dir = ""
        self.time_elapsed = 0
        self.initUI()

    def initUI(self):
        self.setWindowTitle('Universal Data Converter (Excel/CSV to AVRO/Parquet)')
        self.resize(920, 700)
        self.setAcceptDrops(True)

        self.setStyleSheet("""
            QWidget {
                background-color: #f7fafc;
                color: #1f2933;
                font-family: 'Segoe UI';
            }
            QLabel#title {
                font-size: 26px;
                font-weight: 700;
                color: #16324f;
            }
            QLabel#subtitle {
                color: #486581;
                font-size: 14px;
            }
            QFrame#card {
                background: #ffffff;
                border: 1px solid #d8e3ef;
                border-radius: 14px;
            }
            QPushButton {
                background-color: #0f766e;
                color: #ffffff;
                font-size: 13px;
                padding: 10px 14px;
                border: none;
                border-radius: 8px;
                font-weight: 600;
            }
            QPushButton:hover {
                background-color: #0d5f59;
            }
            QPushButton:checked {
                background-color: #1d4ed8;
            }
            QPushButton:disabled {
                background-color: #bcccdc;
                color: #627d98;
            }
            QComboBox {
                border: 1px solid #bcccdc;
                border-radius: 8px;
                padding: 7px;
                background: #ffffff;
            }
            QTextEdit {
                border: 1px solid #243b53;
                border-radius: 12px;
                background-color: #0b243d;
                color: #ffffff;
                font-family: Consolas, monospace;
                font-size: 12px;
            }
        """)

        layout = QVBoxLayout()
        layout.setSpacing(12)

        hero_card = QFrame()
        hero_card.setObjectName("card")
        hero_layout = QVBoxLayout(hero_card)
        hero_layout.setContentsMargins(16, 14, 16, 14)
        hero_layout.setSpacing(4)

        title = QLabel("Universal Data Converter")
        title.setObjectName("title")
        subtitle = QLabel("Convert Excel and CSV files to AVRO and Parquet files")
        subtitle.setObjectName("subtitle")
        hero_layout.addWidget(title)
        hero_layout.addWidget(subtitle)
        layout.addWidget(hero_card)

        # Drag & Drop Zone
        self.drop_zone = QLabel("\nDrop Excel or CSV files here\n")
        self.drop_zone.setAlignment(Qt.AlignCenter)
        self.drop_zone.setStyleSheet("""
            QLabel {
                background-color: #ffffff;
                font-size: 16px;
                color: #486581;
                border: 2px dashed #88a4be;
                border-radius: 12px;
                padding: 18px;
            }
        """)
        layout.addWidget(self.drop_zone)

        self.file_count_label = QLabel("Files selected: 0")
        self.file_count_label.setStyleSheet("font-size: 13px; color: #334e68; font-weight: 600;")
        layout.addWidget(self.file_count_label)

        # Settings Section
        settings_card = QFrame()
        settings_card.setObjectName("card")
        settings_layout = QVBoxLayout(settings_card)
        settings_layout.setContentsMargins(14, 14, 14, 14)
        settings_layout.setSpacing(10)

        top_settings_row = QHBoxLayout()
        
        # Output Folder Selection
        self.folder_btn = QPushButton("Select Output Folder")
        self.folder_btn.clicked.connect(self.select_output_folder)
        self.folder_label = QLabel("Save to: Not selected")
        self.folder_label.setStyleSheet("font-size: 13px; color: #334e68;")
        
        # Format Dropdown
        self.format_label = QLabel("Target Format:")
        self.format_label.setStyleSheet("font-size: 13px; font-weight: bold;")
        self.format_combo = QComboBox()
        self.format_combo.addItems([".avro", ".parquet"])
        self.format_combo.setStyleSheet("font-size: 13px;")

        top_settings_row.addWidget(self.folder_btn)
        top_settings_row.addWidget(self.folder_label, stretch=1)
        top_settings_row.addWidget(self.format_label)
        top_settings_row.addWidget(self.format_combo)
        settings_layout.addLayout(top_settings_row)

        dict_row = QHBoxLayout()
        self.german_dict_btn = QPushButton("German Dictionary: OFF")
        self.german_dict_btn.setCheckable(True)
        self.german_dict_btn.toggled.connect(self.toggle_german_dictionary)
        dict_row.addWidget(self.german_dict_btn)
        dict_row.addStretch(1)
        settings_layout.addLayout(dict_row)

        autodetect_hint = QLabel("Supported: Excel/CSV input and AVRO/Parquet output.")
        autodetect_hint.setStyleSheet("font-size: 12px; color: #486581;")
        settings_layout.addWidget(autodetect_hint)

        layout.addWidget(settings_card)

        # Buttons Layout
        btn_layout = QHBoxLayout()

        # Browse Button
        self.browse_btn = QPushButton("Browse Files")
        self.browse_btn.clicked.connect(self.browse_files)
        btn_layout.addWidget(self.browse_btn)

        # Start Button
        self.start_btn = QPushButton("Start Conversion")
        self.start_btn.setEnabled(False)
        self.start_btn.clicked.connect(self.start_conversion)
        btn_layout.addWidget(self.start_btn)

        layout.addLayout(btn_layout)

        # Timer Label
        self.timer_label = QLabel("Time elapsed: 00:00")
        self.timer_label.setAlignment(Qt.AlignCenter)
        self.timer_label.setStyleSheet("font-size: 15px; font-weight: bold; color: #9f1239;")
        layout.addWidget(self.timer_label)

        # Real-time Status Box
        self.status_box = QTextEdit()
        self.status_box.setReadOnly(True)
        layout.addWidget(self.status_box)

        self.setLayout(layout)
        
        # Initialize Timer
        self.timer = QTimer()
        self.timer.timeout.connect(self.update_timer)

    # --- Drag & Drop Events ---
    def dragEnterEvent(self, event):
        if event.mimeData().hasUrls():
            event.accept()
            self.drop_zone.setStyleSheet("""
                QLabel { background-color: #e6fffa; font-size: 16px; color: #0f172a; border: 2px dashed #0f766e; border-radius: 12px; padding: 18px; }
            """)
        else:
            event.ignore()

    def dragLeaveEvent(self, event):
        self.drop_zone.setStyleSheet("""
            QLabel { background-color: #ffffff; font-size: 16px; color: #486581; border: 2px dashed #88a4be; border-radius: 12px; padding: 18px; }
        """)

    def dropEvent(self, event):
        self.drop_zone.setStyleSheet("""
            QLabel { background-color: #ffffff; font-size: 16px; color: #486581; border: 2px dashed #88a4be; border-radius: 12px; padding: 18px; }
        """)
        for url in event.mimeData().urls():
            file_path = url.toLocalFile()
            if file_path.lower().endswith(('.xls', '.xlsx', '.csv')) and file_path not in self.file_list:
                self.file_list.append(file_path)
        self.check_ready_state()

    # --- File & Folder Selection ---
    def browse_files(self):
        files, _ = QFileDialog.getOpenFileNames(self, "Select Files", "", "Data Files (*.xls *.xlsx *.csv);;All Files (*)")
        if files:
            for file in files:
                if file not in self.file_list:
                    self.file_list.append(file)
            self.check_ready_state()

    def select_output_folder(self):
        folder = QFileDialog.getExistingDirectory(self, "Select Output Folder")
        if folder:
            self.output_dir = folder
            self.folder_label.setText(f"Save to: {self.output_dir}")
            self.check_ready_state()

    # --- Validation ---
    def check_ready_state(self):
        self.file_count_label.setText(f"Files selected: {len(self.file_list)}")
        if self.file_list and self.output_dir:
            self.status_box.setText(
                f"{len(self.file_list)} file(s) ready.\n"
                "Output folder selected.\n"
                "Press 'Start Conversion' when ready.\n"
            )
            self.start_btn.setEnabled(True)
        elif self.file_list and not self.output_dir:
            self.status_box.setText(f"Added {len(self.file_list)} file(s).\nPlease select an output folder to continue.")
            self.start_btn.setEnabled(False)
        else:
            self.start_btn.setEnabled(False)

    def append_status(self, message):
        msg = str(message)
        lower = msg.lower()
        color = '#ffffff'
        if lower.startswith('success'):
            color = '#22c55e'
        elif lower.startswith('warning'):
            color = '#facc15'
        elif lower.startswith('error'):
            color = '#ef4444'

        safe_text = html.escape(msg)
        self.status_box.append(f"<span style='color: {color};'>{safe_text}</span>")

    def toggle_german_dictionary(self, enabled):
        if enabled:
            self.german_dict_btn.setText("German Dictionary: ON")
            self.append_status("Success: German dictionary enabled for headers and records")
        else:
            self.german_dict_btn.setText("German Dictionary: OFF")
            self.append_status("Warning: German dictionary disabled")

    # --- Conversion Logic ---
    def start_conversion(self):
        self.start_btn.setEnabled(False)
        self.browse_btn.setEnabled(False)
        self.folder_btn.setEnabled(False)
        self.format_combo.setEnabled(False)
        
        selected_format = self.format_combo.currentText()
        german_enabled = self.german_dict_btn.isChecked()
        
        self.time_elapsed = 0
        self.timer_label.setText("Time elapsed: 00:00")
        self.append_status("-" * 40)
        self.append_status(f"Starting conversion to {selected_format.upper()} format")
        self.append_status(f"Output directory: {self.output_dir}")
        if german_enabled:
            self.append_status("German dictionary: enabled (headers and records)")
        else:
            self.append_status("German dictionary: disabled")
        
        self.timer.start(1000)

        self.worker = ConverterWorker(
            self.file_list,
            self.output_dir,
            selected_format,
            german_enabled
        )
        self.worker.update_status.connect(self.log_status)
        self.worker.finished.connect(self.conversion_finished)
        self.worker.start()

    def update_timer(self):
        self.time_elapsed += 1
        minutes = self.time_elapsed // 60
        seconds = self.time_elapsed % 60
        self.timer_label.setText(f"Time elapsed: {minutes:02d}:{seconds:02d}")

    def log_status(self, message):
        self.append_status(message)
        scrollbar = self.status_box.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())

    def conversion_finished(self):
        self.timer.stop()
        self.append_status("-" * 40)
        self.append_status("Success: Conversion finished.")
        
        self.file_list = [] 
        self.browse_btn.setEnabled(True)
        self.folder_btn.setEnabled(True)
        self.format_combo.setEnabled(True)
        self.check_ready_state()

# ==========================================
# 3. RUN APPLICATION
# ==========================================
if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = DataConverterApp()
    window.show()
    sys.exit(app.exec_())