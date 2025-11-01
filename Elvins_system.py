"""
ELLVINS Library System - Enhanced Version with Unicode Fix
New features:
- Tooltips on all buttons
- Success messages with green background (no dialogs)
- Dark mode toggle
- Book condition tracking (New, Good, Fair, Poor, Damaged)
- Check for active loans before deleting students/books
- Undo functionality for recent deletions
- Auto-save drafts when adding students/books
- Quantity field for adding multiple copies of same book
- Fixed Unicode encoding issues for Windows console
"""
import sys
import os

# Fix Unicode encoding for Windows console BEFORE any output
if sys.platform == 'win32':
    try:
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
    except (AttributeError, ValueError):
        pass

import sqlite3
import datetime
import tempfile
import json
import hashlib
import hmac
import shutil
from pathlib import Path

try:
    from PyQt5 import QtWidgets, QtGui, QtCore
    from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QLabel,
                                 QPushButton, QLineEdit, QMessageBox, QTableWidget, QTableWidgetItem,
                                 QDialog, QComboBox, QCheckBox, QFileDialog, QSpinBox, QFormLayout, QTextEdit)
    from PyQt5.QtGui import QPixmap, QFont
    from PyQt5.QtCore import Qt, pyqtSignal, QTimer
    print("PyQt5 loaded successfully")
except ImportError as e:
    print(f"ERROR: PyQt5 not found. Please install it: pip install PyQt5")
    print(f"Details: {e}")
    input("Press Enter to exit...")
    sys.exit(1)

# Optional libs
BARCODE_AVAILABLE = False
OPENCV_AVAILABLE = False
REPORTLAB_AVAILABLE = False

try:
    import barcode
    from barcode.writer import ImageWriter
    BARCODE_AVAILABLE = True
    print("Barcode library available")
except:
    print("Barcode library not available (optional)")

try:
    import cv2
    from pyzbar import pyzbar
    OPENCV_AVAILABLE = True
    print("OpenCV available for webcam scanning")
except:
    print("OpenCV not available (optional)")

try:
    from reportlab.lib.pagesizes import letter
    from reportlab.pdfgen import canvas
    REPORTLAB_AVAILABLE = True
    print("ReportLab available for PDF generation")
except:
    print("ReportLab not available (optional)")

# ------------------ Config & Paths ------------------
APPNAME = "ELLVINS"
if os.name == "nt":
    APPDATA = os.getenv("APPDATA") or os.path.expanduser("~")
    DB_DIR = os.path.join(APPDATA, APPNAME)
else:
    DB_DIR = os.path.join(os.path.expanduser("~"), f".{APPNAME.lower()}")

try:
    os.makedirs(DB_DIR, exist_ok=True)
    print(f"Database directory: {DB_DIR}")
except Exception as e:
    print(f"ERROR: Cannot create database directory: {e}")
    input("Press Enter to exit...")
    sys.exit(1)

APP_DB = os.path.join(DB_DIR, "ellvins.db")
LICENSE_FILE = os.path.join(DB_DIR, "license.json")
DRAFTS_FILE = os.path.join(DB_DIR, "drafts.json")

_LICENSE_SECRET = b"ellvins-offline-secret-v1"

DEFAULT_FINE_PER_DAY = 10
DEFAULT_LOAN_DAYS = 14

# Book conditions
BOOK_CONDITIONS = ["New", "Good", "Fair", "Poor", "Damaged"]

# ------------------ Database Manager ------------------
class DB:
    def __init__(self, path=APP_DB):
        self.path = path
        try:
            self.conn = sqlite3.connect(self.path, check_same_thread=False)
            self.conn.row_factory = sqlite3.Row
            self.init_schema()
            print(f"Database initialized: {path}")
        except Exception as e:
            print(f"ERROR: Database initialization failed: {e}")
            raise

    def init_schema(self):
        c = self.conn.cursor()
        c.execute(f"""CREATE TABLE IF NOT EXISTS schools (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE,
            password TEXT,
            created_at TEXT,
            fine_per_day INTEGER DEFAULT {DEFAULT_FINE_PER_DAY},
            default_loan_days INTEGER DEFAULT {DEFAULT_LOAN_DAYS}
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            school_id INTEGER,
            username TEXT,
            password TEXT,
            role TEXT,
            UNIQUE(school_id, username),
            FOREIGN KEY(school_id) REFERENCES schools(id)
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS students (
            id INTEGER PRIMARY KEY,
            school_id INTEGER,
            admission_no TEXT,
            name TEXT,
            class TEXT,
            UNIQUE(school_id, admission_no),
            FOREIGN KEY(school_id) REFERENCES schools(id)
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS books (
            id INTEGER PRIMARY KEY,
            school_id INTEGER,
            title TEXT,
            author TEXT,
            barcode TEXT,
            non_circulating INTEGER DEFAULT 0,
            condition TEXT DEFAULT 'Good',
            UNIQUE(school_id, barcode),
            FOREIGN KEY(school_id) REFERENCES schools(id)
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS loans (
            id INTEGER PRIMARY KEY,
            school_id INTEGER,
            book_id INTEGER,
            student_id INTEGER,
            borrowed_at TEXT,
            due_date TEXT,
            returned_at TEXT,
            fine_paid INTEGER DEFAULT 0,
            FOREIGN KEY(book_id) REFERENCES books(id),
            FOREIGN KEY(student_id) REFERENCES students(id),
            FOREIGN KEY(school_id) REFERENCES schools(id)
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS undo_log (
            id INTEGER PRIMARY KEY,
            school_id INTEGER,
            table_name TEXT,
            record_data TEXT,
            deleted_at TEXT
        )""")
        self.conn.commit()

    def _exec(self, sql, params=()):
        c = self.conn.cursor()
        c.execute(sql, params)
        self.conn.commit()
        return c

    def register_school(self, name, password, fine_per_day=DEFAULT_FINE_PER_DAY, default_loan_days=DEFAULT_LOAN_DAYS):
        try:
            self._exec("INSERT INTO schools (name,password,created_at,fine_per_day,default_loan_days) VALUES (?,?,?,?,?)",
                       (name, password, datetime.datetime.utcnow().isoformat(), fine_per_day, default_loan_days))
            return True
        except sqlite3.IntegrityError:
            return False

    def get_school(self, name):
        c = self._exec("SELECT * FROM schools WHERE name=?", (name,))
        return c.fetchone()

    def get_school_by_id(self, school_id):
        c = self._exec("SELECT * FROM schools WHERE id=?", (school_id,))
        return c.fetchone()

    def create_default_admin_for_school(self, school_id):
        c = self._exec("SELECT * FROM users WHERE school_id=?", (school_id,))
        if not c.fetchone():
            self._exec("INSERT INTO users (school_id,username,password,role) VALUES (?,?,?,?)",
                       (school_id, "admin", "admin", "Admin"))
            return True
        return False

    def validate_school_credentials(self, name, password):
        c = self._exec("SELECT * FROM schools WHERE name=? AND password=?", (name, password))
        return c.fetchone()

    def add_user(self, school_id, username, password, role):
        try:
            self._exec("INSERT INTO users (school_id,username,password,role) VALUES (?,?,?,?)",
                       (school_id, username, password, role))
            return True
        except sqlite3.IntegrityError:
            return False

    def list_users(self, school_id):
        c = self._exec("SELECT * FROM users WHERE school_id=?", (school_id,))
        return c.fetchall()

    def validate_user(self, school_id, username, password):
        c = self._exec("SELECT * FROM users WHERE school_id=? AND username=? AND password=?", (school_id, username, password))
        return c.fetchone()

    def add_student(self, school_id, admission_no, name, klass):
        try:
            self._exec("INSERT INTO students (school_id,admission_no,name,class) VALUES (?,?,?,?)",
                       (school_id, admission_no, name, klass))
            return True
        except sqlite3.IntegrityError:
            return False

    def get_unique_classes(self, school_id):
        c = self._exec("SELECT DISTINCT class FROM students WHERE school_id=? AND class IS NOT NULL AND class != '' ORDER BY class", (school_id,))
        return [row["class"] for row in c.fetchall()]

    def delete_student(self, school_id, admission_no):
        c = self._exec("SELECT * FROM students WHERE school_id=? AND admission_no=?", (school_id, admission_no))
        student = c.fetchone()
        if student:
            record_data = json.dumps(dict(student))
            self._exec("INSERT INTO undo_log (school_id, table_name, record_data, deleted_at) VALUES (?,?,?,?)",
                       (school_id, "students", record_data, datetime.datetime.utcnow().isoformat()))
        self._exec("DELETE FROM students WHERE school_id=? AND admission_no=?", (school_id, admission_no))
    
    def has_active_loans_student(self, school_id, admission_no):
        c = self._exec("""SELECT COUNT(*) as count FROM loans 
                         JOIN students ON students.id = loans.student_id
                         WHERE loans.school_id=? AND students.admission_no=? AND loans.returned_at IS NULL""",
                       (school_id, admission_no))
        return c.fetchone()["count"] > 0

    def list_students(self, school_id):
        c = self._exec("SELECT * FROM students WHERE school_id=? ORDER BY name", (school_id,))
        return c.fetchall()

    def find_student(self, school_id, admission_no):
        c = self._exec("SELECT * FROM students WHERE school_id=? AND admission_no=?", (school_id, admission_no))
        return c.fetchone()

    def search_students(self, school_id, search_term):
        c = self._exec("""SELECT * FROM students 
                         WHERE school_id=? AND (admission_no LIKE ? OR name LIKE ?)
                         ORDER BY name""", 
                       (school_id, f"%{search_term}%", f"%{search_term}%"))
        return c.fetchall()

    def add_book(self, school_id, title, author, barcode_val, non_circ=False, condition="Good"):
        try:
            self._exec("INSERT INTO books (school_id,title,author,barcode,non_circulating,condition) VALUES (?,?,?,?,?,?)",
                       (school_id, title, author, barcode_val, 1 if non_circ else 0, condition))
            return True
        except sqlite3.IntegrityError:
            return False

    def get_unique_authors(self, school_id):
        c = self._exec("SELECT DISTINCT author FROM books WHERE school_id=? AND author IS NOT NULL AND author != '' ORDER BY author", (school_id,))
        return [row["author"] for row in c.fetchall()]

    def delete_book(self, school_id, barcode_val):
        c = self._exec("SELECT * FROM books WHERE school_id=? AND barcode=?", (school_id, barcode_val))
        book = c.fetchone()
        if book:
            record_data = json.dumps(dict(book))
            self._exec("INSERT INTO undo_log (school_id, table_name, record_data, deleted_at) VALUES (?,?,?,?)",
                       (school_id, "books", record_data, datetime.datetime.utcnow().isoformat()))
        self._exec("DELETE FROM books WHERE school_id=? AND barcode=?", (school_id, barcode_val))
    
    def has_active_loans_book(self, school_id, barcode_val):
        c = self._exec("""SELECT COUNT(*) as count FROM loans 
                         JOIN books ON books.id = loans.book_id
                         WHERE loans.school_id=? AND books.barcode=? AND loans.returned_at IS NULL""",
                       (school_id, barcode_val))
        return c.fetchone()["count"] > 0

    def list_books(self, school_id):
        c = self._exec("SELECT * FROM books WHERE school_id=? ORDER BY title", (school_id,))
        return c.fetchall()

    def find_book(self, school_id, barcode_val):
        c = self._exec("SELECT * FROM books WHERE school_id=? AND barcode=?", (school_id, barcode_val))
        return c.fetchone()

    def get_recent_deletions(self, school_id, limit=10):
        c = self._exec("SELECT * FROM undo_log WHERE school_id=? ORDER BY deleted_at DESC LIMIT ?", (school_id, limit))
        return c.fetchall()
    
    def undo_deletion(self, school_id, undo_id):
        c = self._exec("SELECT * FROM undo_log WHERE id=? AND school_id=?", (undo_id, school_id))
        record = c.fetchone()
        if not record:
            return False, "Undo record not found"
        
        table_name = record["table_name"]
        data = json.loads(record["record_data"])
        
        try:
            if table_name == "students":
                self.add_student(school_id, data["admission_no"], data["name"], data["class"])
            elif table_name == "books":
                self.add_book(school_id, data["title"], data["author"], data["barcode"], 
                             data["non_circulating"], data.get("condition", "Good"))
            self._exec("DELETE FROM undo_log WHERE id=?", (undo_id,))
            return True, "Record restored"
        except Exception as e:
            return False, f"Failed to restore: {e}"

    def borrow_book(self, school_id, book_id, student_id, days=None):
        c = self.conn.cursor()
        c.execute("SELECT * FROM loans WHERE school_id=? AND book_id=? AND returned_at IS NULL", (school_id, book_id))
        if c.fetchone():
            return False, "Book already borrowed"
        borrowed_at = datetime.datetime.utcnow()
        if days is None:
            r = self._exec("SELECT default_loan_days FROM schools WHERE id=?", (school_id,)).fetchone()
            days = r["default_loan_days"] if r else DEFAULT_LOAN_DAYS
        due = borrowed_at + datetime.timedelta(days=int(days))
        c.execute("INSERT INTO loans (school_id,book_id,student_id,borrowed_at,due_date) VALUES (?,?,?,?,?)",
                  (school_id, book_id, student_id, borrowed_at.isoformat(), due.isoformat()))
        self.conn.commit()
        return True, "Borrow recorded"

    def return_book(self, school_id, book_id):
        c = self.conn.cursor()
        c.execute("SELECT * FROM loans WHERE school_id=? AND book_id=? AND returned_at IS NULL", (school_id, book_id))
        r = c.fetchone()
        if not r:
            return False, "No active loan"
        returned_at = datetime.datetime.utcnow().isoformat()
        c.execute("UPDATE loans SET returned_at=? WHERE id=?", (returned_at, r["id"]))
        self.conn.commit()
        due = datetime.datetime.fromisoformat(r["due_date"])
        ret = datetime.datetime.fromisoformat(returned_at)
        days_late = (ret.date() - due.date()).days
        if days_late > 0:
            school = self._exec("SELECT fine_per_day FROM schools WHERE id=?", (school_id,)).fetchone()
            fine_per_day = school["fine_per_day"] if school else DEFAULT_FINE_PER_DAY
            fine = days_late * fine_per_day
            return True, f"Returned. Fine due: {fine} (days late: {days_late})"
        return True, "Returned. No fine."

    def current_loans(self, school_id):
        c = self._exec("""SELECT loans.*, books.title, books.barcode, books.condition, students.admission_no, students.name as student_name
                     FROM loans
                     JOIN books ON books.id = loans.book_id
                     JOIN students ON students.id = loans.student_id
                     WHERE loans.school_id=? AND loans.returned_at IS NULL""", (school_id,))
        return c.fetchall()

    def get_student_active_loans(self, school_id, student_id):
        c = self._exec("""SELECT loans.*, books.title, books.barcode, students.admission_no, students.name as student_name
                     FROM loans
                     JOIN books ON books.id = loans.book_id
                     JOIN students ON students.id = loans.student_id
                     WHERE loans.school_id=? AND loans.student_id=? AND loans.returned_at IS NULL
                     ORDER BY loans.due_date""", (school_id, student_id))
        return c.fetchall()

    def loan_history(self, school_id):
        c = self._exec("""SELECT loans.*, books.title, books.barcode, students.admission_no, students.name as student_name
                     FROM loans
                     JOIN books ON books.id = loans.book_id
                     JOIN students ON students.id = loans.student_id
                     WHERE loans.school_id=? ORDER BY loans.borrowed_at DESC""", (school_id,))
        return c.fetchall()

    def get_student_loan_history(self, school_id, student_id):
        c = self._exec("""SELECT loans.*, books.title, books.barcode
                     FROM loans
                     JOIN books ON books.id = loans.book_id
                     WHERE loans.school_id=? AND loans.student_id=?
                     ORDER BY loans.borrowed_at DESC""", (school_id, student_id))
        return c.fetchall()

    def update_school_settings(self, school_id, fine_per_day, default_loan_days):
        self._exec("UPDATE schools SET fine_per_day=?, default_loan_days=? WHERE id=?", (fine_per_day, default_loan_days, school_id))

# ------------------ Draft Manager ------------------
class DraftManager:
    @staticmethod
    def save_draft(category, data):
        try:
            drafts = {}
            if os.path.exists(DRAFTS_FILE):
                with open(DRAFTS_FILE, 'r', encoding='utf-8') as f:
                    drafts = json.load(f)
            drafts[category] = data
            with open(DRAFTS_FILE, 'w', encoding='utf-8') as f:
                json.dump(drafts, f)
        except Exception as e:
            print(f"Failed to save draft: {e}")
    
    @staticmethod
    def load_draft(category):
        try:
            if os.path.exists(DRAFTS_FILE):
                with open(DRAFTS_FILE, 'r', encoding='utf-8') as f:
                    drafts = json.load(f)
                return drafts.get(category, {})
        except Exception as e:
            print(f"Failed to load draft: {e}")
        return {}
    
    @staticmethod
    def clear_draft(category):
        try:
            if os.path.exists(DRAFTS_FILE):
                with open(DRAFTS_FILE, 'r', encoding='utf-8') as f:
                    drafts = json.load(f)
                if category in drafts:
                    del drafts[category]
                with open(DRAFTS_FILE, 'w', encoding='utf-8') as f:
                    json.dump(drafts, f)
        except Exception as e:
            print(f"Failed to clear draft: {e}")

# ------------------ Success Message Widget ------------------
class SuccessMessage(QLabel):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setStyleSheet("""
            background-color: #4CAF50;
            color: white;
            padding: 12px;
            border-radius: 6px;
            font-weight: bold;
            font-size: 13px;
        """)
        self.setAlignment(Qt.AlignCenter)
        self.hide()
    
    def show_message(self, message, duration=3000):
        self.setText(f"✓ {message}")
        self.show()
        QTimer.singleShot(duration, self.hide)

# ------------------ Licensing Utilities ------------------
def validate_license_file(path, expected_school_name):
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if data.get("school") != expected_school_name:
            return False, "License file school name mismatch"
        mac = data.get("mac", "")
        expected_mac = hmac.new(_LICENSE_SECRET, expected_school_name.encode("utf-8"), hashlib.sha256).hexdigest()
        if hmac.compare_digest(mac, expected_mac):
            return True, "License valid"
        else:
            return False, "License HMAC invalid"
    except Exception as e:
        return False, f"Failed to read license: {e}"

# ------------------ Webcam scanner thread ------------------
if OPENCV_AVAILABLE:
    class WebcamScanner(QtCore.QThread):
        barcode_detected = pyqtSignal(str)
        
        def __init__(self):
            super().__init__()
            self._running = True

        def run(self):
            cap = None
            try:
                cap = cv2.VideoCapture(0)
                if not cap.isOpened():
                    return
                
                while self._running:
                    ret, frame = cap.read()
                    if not ret:
                        break
                        
                    barcodes = pyzbar.decode(frame)
                    for b in barcodes:
                        code = b.data.decode('utf-8')
                        self.barcode_detected.emit(code)
                        self._running = False
                        break
                        
                    cv2.waitKey(30)
                    
            except Exception as e:
                print(f"Webcam scanner error: {e}")
            finally:
                if cap is not None:
                    cap.release()
                cv2.destroyAllWindows()

        def stop(self):
            self._running = False
            self.wait()

# ------------------ Student Search Dialog ------------------
class StudentSearchDialog(QDialog):
    def __init__(self, db, school_id, school_row):
        super().__init__()
        self.db = db
        self.school_id = school_id
        self.school_row = school_row
        self.setWindowTitle("Student Loan Search")
        self.setMinimumSize(800, 600)
        self.setup_ui()

    def setup_ui(self):
        layout = QVBoxLayout()
        
        search_layout = QHBoxLayout()
        search_layout.addWidget(QLabel("Search Student:"))
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Enter admission number or student name...")
        self.search_input.textChanged.connect(self.on_search)
        search_layout.addWidget(self.search_input)
        btn_search = QPushButton("Search")
        btn_search.clicked.connect(self.perform_search)
        search_layout.addWidget(btn_search)
        layout.addLayout(search_layout)
        
        self.results_text = QTextEdit()
        self.results_text.setReadOnly(True)
        self.results_text.setStyleSheet("font-family: Consolas, monospace; font-size: 11pt;")
        layout.addWidget(self.results_text)
        
        btn_close = QPushButton("Close")
        btn_close.clicked.connect(self.accept)
        layout.addWidget(btn_close)
        
        self.setLayout(layout)

    def on_search(self):
        if len(self.search_input.text().strip()) >= 2:
            self.perform_search()

    def perform_search(self):
        search_term = self.search_input.text().strip()
        if not search_term:
            self.results_text.setPlainText("Please enter a search term")
            return
        
        students = self.db.search_students(self.school_id, search_term)
        
        if not students:
            self.results_text.setPlainText(f"No students found matching '{search_term}'")
            return
        
        result_text = f"SEARCH RESULTS FOR: '{search_term}'\n"
        result_text += "=" * 80 + "\n\n"
        
        for student in students:
            result_text += f"STUDENT: {student['name']}\n"
            result_text += f"Admission No: {student['admission_no']}\n"
            result_text += f"Class: {student['class']}\n"
            result_text += "-" * 80 + "\n"
            
            active_loans = self.db.get_student_active_loans(self.school_id, student['id'])
            
            if active_loans:
                result_text += f"ACTIVE LOANS ({len(active_loans)} book(s)):\n\n"
                total_fine = 0
                
                for loan in active_loans:
                    result_text += f"  - {loan['title']} (Barcode: {loan['barcode']})\n"
                    result_text += f"    Borrowed: {loan['borrowed_at'][:10]}\n"
                    result_text += f"    Due Date: {loan['due_date'][:10]}\n"
                    
                    due_date = datetime.datetime.fromisoformat(loan['due_date'])
                    now = datetime.datetime.utcnow()
                    days_late = (now.date() - due_date.date()).days
                    
                    if days_late > 0:
                        fine_per_day = self.school_row['fine_per_day']
                        fine = days_late * fine_per_day
                        total_fine += fine
                        result_text += f"    [!] OVERDUE by {days_late} day(s) - Fine: {fine} units\n"
                    else:
                        days_remaining = -days_late
                        result_text += f"    [OK] Due in {days_remaining} day(s)\n"
                    result_text += "\n"
                
                if total_fine > 0:
                    result_text += f"TOTAL FINES DUE: {total_fine} units\n"
                else:
                    result_text += "No fines.\n"
            else:
                result_text += "No active loans.\n"
            
            history = self.db.get_student_loan_history(self.school_id, student['id'])
            result_text += f"\nTotal History: {len(history)} book(s)\n"
            result_text += "\n" + "=" * 80 + "\n\n"
        
        self.results_text.setPlainText(result_text)

# ------------------ Undo Dialog ------------------
class UndoDialog(QDialog):
    def __init__(self, db, school_id, parent=None):
        super().__init__(parent)
        self.db = db
        self.school_id = school_id
        self.setWindowTitle("Undo Recent Deletions")
        self.setMinimumSize(600, 400)
        self.setup_ui()
        
    def setup_ui(self):
        layout = QVBoxLayout()
        
        layout.addWidget(QLabel("Recent Deletions (Last 10):"))
        
        self.table = QTableWidget(0, 4)
        self.table.setHorizontalHeaderLabels(["Type", "Details", "Deleted At", "Action"])
        self.table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.table)
        
        btn_close = QPushButton("Close")
        btn_close.clicked.connect(self.accept)
        layout.addWidget(btn_close)
        
        self.setLayout(layout)
        self.refresh()
    
    def refresh(self):
        deletions = self.db.get_recent_deletions(self.school_id)
        self.table.setRowCount(0)
        
        for deletion in deletions:
            row = self.table.rowCount()
            self.table.insertRow(row)
            
            data = json.loads(deletion["record_data"])
            table_type = deletion["table_name"]
            
            if table_type == "students":
                details = f"{data['name']} ({data['admission_no']})"
            elif table_type == "books":
                details = f"{data['title']} - {data['barcode']}"
            else:
                details = "Unknown"
            
            self.table.setItem(row, 0, QTableWidgetItem(table_type.title()))
            self.table.setItem(row, 1, QTableWidgetItem(details))
            self.table.setItem(row, 2, QTableWidgetItem(deletion["deleted_at"][:19]))
            
            btn_undo = QPushButton("Undo")
            btn_undo.clicked.connect(lambda _, uid=deletion["id"]: self.undo_deletion(uid))
            self.table.setCellWidget(row, 3, btn_undo)
    
    def undo_deletion(self, undo_id):
        ok, msg = self.db.undo_deletion(self.school_id, undo_id)
        if ok:
            QMessageBox.information(self, "Success", msg)
            self.refresh()
            if self.parent():
                self.parent().refresh_current_page()
        else:
            QMessageBox.warning(self, "Error", msg)

# ------------------ GUI Components ------------------
class ActivationDialog(QDialog):
    def __init__(self, school_name):
        super().__init__()
        self.setWindowTitle("ELLVINS - Offline Activation")
        self.setFixedSize(480,180)
        self.school_name = school_name
        layout = QVBoxLayout()
        layout.addWidget(QLabel(f"Activate license for school: {school_name}"))
        self.lbl_status = QLabel("License status: Not checked")
        layout.addWidget(self.lbl_status)
        btn_choose = QPushButton("Load license file...")
        btn_choose.clicked.connect(self.load_license)
        layout.addWidget(btn_choose)
        self.setLayout(layout)
        self.activated = False

    def load_license(self):
        path, _ = QFileDialog.getOpenFileName(self, "Select license JSON file", "", "JSON files (*.json);;All files (*)")
        if not path:
            return
        ok, msg = validate_license_file(path, self.school_name)
        if ok:
            shutil.copy(path, LICENSE_FILE)
            self.lbl_status.setText("License status: Activated")
            QMessageBox.information(self, "Activated", "License activated successfully.")
            self.activated = True
            self.accept()
        else:
            QMessageBox.warning(self, "Activation failed", msg)

class LoginDialog(QDialog):
    def __init__(self, db):
        super().__init__()
        self.db = db
        self.school = None
        self.user = None
        self.school_row = None
        self.setWindowTitle("ELLVINS Library System - Login")
        self.setFixedSize(520,300)
        main = QVBoxLayout()
        heading = QLabel("ELLVINS Library System")
        heading.setFont(QFont("Arial", 16, QFont.Bold))
        heading.setAlignment(Qt.AlignCenter)
        heading.setStyleSheet("background:#0B61A4; color:white; padding:8px; border-radius:6px;")
        main.addWidget(heading)

        form = QFormLayout()
        self.school_name = QLineEdit(); self.school_name.setPlaceholderText("School name")
        self.school_pw = QLineEdit(); self.school_pw.setEchoMode(QLineEdit.Password)
        form.addRow("School name:", self.school_name)
        form.addRow("School password:", self.school_pw)
        self.user_combo = QComboBox()
        self.user_combo.setEditable(True)
        self.user_pw = QLineEdit(); self.user_pw.setEchoMode(QLineEdit.Password)
        form.addRow("User:", self.user_combo)
        form.addRow("User password:", self.user_pw)
        main.addLayout(form)

        btns = QHBoxLayout()
        btn_school_login = QPushButton("School Login")
        btn_school_register = QPushButton("Register School")
        btn_load_users = QPushButton("Load users")
        btn_school_login.clicked.connect(self.school_login)
        btn_school_register.clicked.connect(self.register_school)
        btn_load_users.clicked.connect(self.load_users_for_school)
        btns.addWidget(btn_school_login); btns.addWidget(btn_school_register); btns.addWidget(btn_load_users)
        main.addLayout(btns)

        user_btns = QHBoxLayout()
        btn_user_login = QPushButton("Login User")
        btn_user_create = QPushButton("Create User")
        btn_user_login.clicked.connect(self.user_login)
        btn_user_create.clicked.connect(self.create_user)
        user_btns.addWidget(btn_user_login); user_btns.addWidget(btn_user_create)
        main.addLayout(user_btns)

        hint = QLabel("Tip: Register school first, then create admin user.")
        hint.setStyleSheet("color:gray; font-size:11px;")
        main.addWidget(hint)

        self.setLayout(main)

    def register_school(self):
        name = self.school_name.text().strip()
        pw = self.school_pw.text().strip()
        if not name or not pw:
            QMessageBox.warning(self, "Input", "Enter school name and password")
            return
        created = self.db.register_school(name, pw)
        if not created:
            QMessageBox.warning(self, "Register failed", "School already exists")
            return
        school = self.db.get_school(name)
        self.db.create_default_admin_for_school(school["id"])
        QMessageBox.information(self, "Registered", f"School '{name}' registered.\nDefault admin: username='admin', password='admin'")

    def school_login(self):
        name = self.school_name.text().strip()
        pw = self.school_pw.text().strip()
        if not name or not pw:
            QMessageBox.warning(self, "Input", "Enter school name and password")
            return
        sch = self.db.validate_school_credentials(name, pw)
        if not sch:
            QMessageBox.warning(self, "Login failed", "Wrong school credentials")
            return
        self.school = sch["name"]
        self.school_row = sch
        QMessageBox.information(self, "School login", f"Logged in as school: {self.school}. Now login as a user.")
        self.load_users_for_school()

    def load_users_for_school(self):
        name = self.school_name.text().strip()
        if not name:
            QMessageBox.warning(self, "Input", "Enter school name first")
            return
        sch = self.db.get_school(name)
        if not sch:
            QMessageBox.warning(self, "Not found", "School not registered yet")
            return
        users = self.db.list_users(sch["id"])
        self.user_combo.clear()
        for u in users:
            self.user_combo.addItem(u["username"])
        self.school_row = sch

    def create_user(self):
        if not self.school_row:
            QMessageBox.warning(self, "Input", "Load or login a school first")
            return
        username = self.user_combo.currentText().strip()
        pw = self.user_pw.text().strip()
        if not username or not pw:
            QMessageBox.warning(self, "Input", "Enter username and password")
            return
        role, ok = QtWidgets.QInputDialog.getItem(self, "Select role", "Role:", ["Admin", "Librarian", "Assistant"], 0, False)
        if not ok:
            return
        created = self.db.add_user(self.school_row["id"], username, pw, role)
        if not created:
            QMessageBox.warning(self, "Error", "User likely exists")
        else:
            QMessageBox.information(self, "User created", f"User {username} created with role {role}")
            self.load_users_for_school()

    def user_login(self):
        if not self.school_row:
            QMessageBox.warning(self, "Input", "Load or login a school first")
            return
        username = self.user_combo.currentText().strip()
        pw = self.user_pw.text().strip()
        if not username or not pw:
            QMessageBox.warning(self, "Input", "Enter username and password")
            return
        row = self.db.validate_user(self.school_row["id"], username, pw)
        if not row:
            QMessageBox.warning(self, "Login failed", "Invalid user credentials")
            return
        self.user = row
        self.accept()

class MainWindow(QMainWindow):
    def __init__(self, db, school_row, user_row):
        super().__init__()
        self.db = db
        self.school = school_row
        self.school_id = school_row["id"]
        self.user = user_row
        self.cam_running = False
        self._cam_thread = None
        self.dark_mode = False
        
        # Initialize these variables before building UI
        self.last_student_class = ""
        self.last_book_author = ""
        
        self.setWindowTitle(f"ELLVINS Library System - {self.school['name']} [{self.user['username']} - {self.user['role']}]")
        self.setMinimumSize(1100, 700)
        self.setup_ui()
        self.apply_theme()

    def closeEvent(self, event):
        if self.cam_running and self._cam_thread:
            self._cam_thread.stop()
        event.accept()

    def apply_theme(self):
        if self.dark_mode:
            self.setStyleSheet("""
                QMainWindow, QWidget { background-color: #2b2b2b; color: #e0e0e0; }
                QLineEdit, QComboBox, QSpinBox, QTextEdit { 
                    background-color: #3c3c3c; 
                    color: #e0e0e0; 
                    border: 1px solid #555;
                    padding: 5px;
                }
                QPushButton { 
                    background-color: #0B61A4; 
                    color: white; 
                    border: none; 
                    padding: 8px;
                    border-radius: 4px;
                }
                QPushButton:hover { background-color: #0d7bc7; }
                QTableWidget { 
                    background-color: #3c3c3c; 
                    color: #e0e0e0;
                    gridline-color: #555;
                }
                QHeaderView::section { 
                    background-color: #444; 
                    color: #e0e0e0;
                    border: 1px solid #555;
                }
                QLabel { color: #e0e0e0; }
            """)
        else:
            self.setStyleSheet("""
                QMainWindow, QWidget { background-color: #f5f5f5; color: #333; }
                QLineEdit, QComboBox, QSpinBox, QTextEdit { 
                    background-color: white; 
                    border: 1px solid #ddd;
                    padding: 5px;
                }
                QPushButton { 
                    background-color: #E8F3FF; 
                    border: 1px solid #cfeaff;
                    padding: 8px;
                    border-radius: 4px;
                }
                QPushButton:hover { background-color: #d0e8ff; }
                QTableWidget { 
                    background-color: white;
                    gridline-color: #ddd;
                }
            """)

    def toggle_dark_mode(self):
        self.dark_mode = not self.dark_mode
        self.apply_theme()

    def setup_ui(self):
        main_widget = QWidget()
        main_layout = QVBoxLayout()
        main_widget.setLayout(main_layout)
        self.setCentralWidget(main_widget)

        header = QLabel(f"ELLVINS Library System - {self.school['name']}  |  User: {self.user['username']} ({self.user['role']})")
        header.setFont(QFont("Arial", 14, QFont.Bold))
        header.setStyleSheet("background:#0B61A4; color:white; padding:12px; border-radius:6px;")
        header.setAlignment(Qt.AlignCenter)
        main_layout.addWidget(header)
        
        self.success_msg = SuccessMessage()
        main_layout.addWidget(self.success_msg)

        menu = QHBoxLayout()
        self.btn_students = QPushButton("Students")
        self.btn_books = QPushButton("Books")
        self.btn_borrow = QPushButton("Borrow/Return")
        self.btn_loans = QPushButton("Active Loans")
        self.btn_history = QPushButton("History")
        self.btn_search = QPushButton("Search Student")
        self.btn_undo = QPushButton("Undo")
        self.btn_settings = QPushButton("Settings")
        self.btn_backup = QPushButton("Backup")
        self.btn_dark = QPushButton("Dark Mode")
        self.btn_quit = QPushButton("Quit")
        
        for b in (self.btn_students, self.btn_books, self.btn_borrow, self.btn_loans, 
                  self.btn_history, self.btn_search, self.btn_undo, 
                  self.btn_settings, self.btn_backup, self.btn_dark, self.btn_quit):
            b.setMinimumHeight(35)
            menu.addWidget(b)
        
        main_layout.addLayout(menu)

        self.stack = QtWidgets.QStackedWidget()
        main_layout.addWidget(self.stack)

        self.page_students = self.build_students_page()
        self.page_books = self.build_books_page()
        self.page_borrow = self.build_borrow_page()
        self.page_loans = self.build_loans_page()
        self.page_history = self.build_history_page()
        self.page_settings = self.build_settings_page()
        self.page_backup = self.build_backup_page()

        for p in (self.page_students, self.page_books, self.page_borrow, self.page_loans, 
                  self.page_history, self.page_settings, self.page_backup):
            self.stack.addWidget(p)

        self.btn_students.clicked.connect(lambda: self.show_page(0))
        self.btn_books.clicked.connect(lambda: self.show_page(1))
        self.btn_borrow.clicked.connect(lambda: self.show_page(2))
        self.btn_loans.clicked.connect(lambda: self.show_page(3))
        self.btn_history.clicked.connect(lambda: self.show_page(4))
        self.btn_search.clicked.connect(self.open_student_search)
        self.btn_undo.clicked.connect(self.open_undo_dialog)
        self.btn_settings.clicked.connect(lambda: self.show_page(5))
        self.btn_backup.clicked.connect(lambda: self.show_page(6))
        self.btn_dark.clicked.connect(self.toggle_dark_mode)
        self.btn_quit.clicked.connect(self.close)

        self.show_page(0)

    def show_page(self, idx):
        self.stack.setCurrentIndex(idx)
        if idx == 0:
            self.refresh_students()
        elif idx == 1:
            self.refresh_books()
        elif idx == 2:
            self.refresh_borrow_lists()
        elif idx == 3:
            self.refresh_loans()
        elif idx == 4:
            self.refresh_history()
        elif idx == 5:
            self.load_settings()
    
    def refresh_current_page(self):
        idx = self.stack.currentIndex()
        self.show_page(idx)

    def open_student_search(self):
        dlg = StudentSearchDialog(self.db, self.school_id, self.school)
        dlg.exec_()
    
    def open_undo_dialog(self):
        dlg = UndoDialog(self.db, self.school_id, self)
        dlg.exec_()

    def build_students_page(self):
        w = QWidget(); l = QVBoxLayout(); w.setLayout(l)
        
        top = QHBoxLayout()
        self.s_adm = QLineEdit(); self.s_adm.setPlaceholderText("Admission No.")
        self.s_name = QLineEdit(); self.s_name.setPlaceholderText("Student name")
        self.s_class = QComboBox()
        self.s_class.setEditable(True)
        self.s_class.lineEdit().setPlaceholderText("Class")
        
        btn_add = QPushButton("Add Student")
        btn_add.clicked.connect(self.add_student)
        btn_del = QPushButton("Delete")
        btn_del.clicked.connect(self.delete_student)
        
        top.addWidget(self.s_adm); top.addWidget(self.s_name); top.addWidget(self.s_class)
        top.addWidget(btn_add); top.addWidget(btn_del)
        l.addLayout(top)
        
        self.table_students = QTableWidget(0,3)
        self.table_students.setHorizontalHeaderLabels(["Admission No","Name","Class"])
        self.table_students.horizontalHeader().setStretchLastSection(True)
        self.table_students.cellClicked.connect(self.on_student_cell_clicked)
        l.addWidget(self.table_students)
        
        return w
    
    def on_student_cell_clicked(self, row, col):
        self.s_adm.setText(self.table_students.item(row, 0).text())
        self.s_name.setText(self.table_students.item(row, 1).text())
        self.s_class.setCurrentText(self.table_students.item(row, 2).text())

    def refresh_students(self):
        rows = self.db.list_students(self.school_id)
        self.table_students.setRowCount(0)
        for r in rows:
            row = self.table_students.rowCount()
            self.table_students.insertRow(row)
            self.table_students.setItem(row,0,QTableWidgetItem(r["admission_no"]))
            self.table_students.setItem(row,1,QTableWidgetItem(r["name"]))
            self.table_students.setItem(row,2,QTableWidgetItem(r["class"] or ""))
        
        unique_classes = self.db.get_unique_classes(self.school_id)
        self.s_class.clear()
        self.s_class.addItems(unique_classes)

    def add_student(self):
        adm = self.s_adm.text().strip()
        name = self.s_name.text().strip()
        klass = self.s_class.currentText().strip()
        if not adm or not name:
            self.success_msg.show_message("Admission number and name required", 3000)
            return
        ok = self.db.add_student(self.school_id, adm, name, klass)
        if not ok:
            self.success_msg.show_message("Student already exists", 3000)
        else:
            self.refresh_students()
            self.s_adm.clear()
            self.s_name.clear()
            self.s_adm.setFocus()
            self.success_msg.show_message(f"Student {name} added!")

    def delete_student(self):
        if self.user["role"] not in ["Admin", "Librarian"]:
            self.success_msg.show_message("Permission denied", 3000)
            return
        adm = self.s_adm.text().strip()
        if not adm:
            self.success_msg.show_message("Enter admission no", 3000)
            return
        
        if self.db.has_active_loans_student(self.school_id, adm):
            QMessageBox.warning(self,"Cannot Delete", "Student has active loans")
            return
        
        reply = QMessageBox.question(self, 'Delete', f'Delete student {adm}?',
                                     QMessageBox.Yes | QMessageBox.No)
        if reply == QMessageBox.Yes:
            self.db.delete_student(self.school_id, adm)
            self.refresh_students()
            self.success_msg.show_message(f"Student deleted")

    def build_books_page(self):
        w = QWidget(); l = QVBoxLayout(); w.setLayout(l)
        
        top = QHBoxLayout()
        self.b_title = QLineEdit(); self.b_title.setPlaceholderText("Title")
        self.b_author = QComboBox()
        self.b_author.setEditable(True)
        self.b_author.lineEdit().setPlaceholderText("Author")
        self.b_barcode = QLineEdit(); self.b_barcode.setPlaceholderText("Barcode")
        self.b_condition = QComboBox()
        self.b_condition.addItems(BOOK_CONDITIONS)
        self.b_condition.setCurrentText("Good")
        self.b_non = QCheckBox("Non-circ")
        
        btn_add = QPushButton("Add Book")
        btn_add.clicked.connect(self.add_book)
        btn_del = QPushButton("Delete")
        btn_del.clicked.connect(self.delete_book)
        
        top.addWidget(self.b_title); top.addWidget(self.b_author)
        top.addWidget(self.b_barcode); top.addWidget(self.b_condition)
        top.addWidget(self.b_non); top.addWidget(btn_add); top.addWidget(btn_del)
        l.addLayout(top)
        
        self.table_books = QTableWidget(0,5)
        self.table_books.setHorizontalHeaderLabels(["Title","Author","Barcode","Condition","Non-circ"])
        self.table_books.horizontalHeader().setStretchLastSection(True)
        self.table_books.cellClicked.connect(self.on_book_cell_clicked)
        l.addWidget(self.table_books)
        
        return w
    
    def on_book_cell_clicked(self, row, col):
        self.b_title.setText(self.table_books.item(row, 0).text())
        self.b_author.setCurrentText(self.table_books.item(row, 1).text())
        self.b_barcode.setText(self.table_books.item(row, 2).text())
        self.b_condition.setCurrentText(self.table_books.item(row, 3).text())
        non_circ = self.table_books.item(row, 4).text() == "Yes"
        self.b_non.setChecked(non_circ)

    def refresh_books(self):
        rows = self.db.list_books(self.school_id)
        self.table_books.setRowCount(0)
        for r in rows:
            row = self.table_books.rowCount()
            self.table_books.insertRow(row)
            self.table_books.setItem(row,0,QTableWidgetItem(r["title"]))
            self.table_books.setItem(row,1,QTableWidgetItem(r["author"] or ""))
            self.table_books.setItem(row,2,QTableWidgetItem(r["barcode"]))
            self.table_books.setItem(row,3,QTableWidgetItem(r["condition"]))
            self.table_books.setItem(row,4,QTableWidgetItem("Yes" if r["non_circulating"] else "No"))
        
        unique_authors = self.db.get_unique_authors(self.school_id)
        self.b_author.clear()
        self.b_author.addItems(unique_authors)

    def add_book(self):
        if self.user["role"] == "Assistant":
            self.success_msg.show_message("Permission denied", 3000)
            return
        
        title = self.b_title.text().strip()
        author = self.b_author.currentText().strip()
        barcode_val = self.b_barcode.text().strip()
        non = self.b_non.isChecked()
        condition = self.b_condition.currentText()
        
        if not title or not barcode_val:
            self.success_msg.show_message("Title and barcode required", 3000)
            return
        
        ok = self.db.add_book(self.school_id, title, author, barcode_val, non, condition)
        if not ok:
            self.success_msg.show_message("Book exists (duplicate barcode)", 3000)
        else:
            self.refresh_books()
            self.b_title.clear()
            self.b_barcode.clear()
            self.b_title.setFocus()
            self.success_msg.show_message(f"Book '{title}' added!")

    def delete_book(self):
        if self.user["role"] != "Admin":
            self.success_msg.show_message("Only Admin can delete", 3000)
            return
        bc = self.b_barcode.text().strip()
        if not bc:
            self.success_msg.show_message("Enter barcode", 3000)
            return
        
        if self.db.has_active_loans_book(self.school_id, bc):
            QMessageBox.warning(self,"Cannot Delete", "Book is on loan")
            return
        
        reply = QMessageBox.question(self, 'Delete', f'Delete book {bc}?',
                                     QMessageBox.Yes | QMessageBox.No)
        if reply == QMessageBox.Yes:
            self.db.delete_book(self.school_id, bc)
            self.refresh_books()
            self.success_msg.show_message("Book deleted")

    def build_borrow_page(self):
        w = QWidget(); l = QVBoxLayout(); w.setLayout(l)
        top = QHBoxLayout()
        self.borrow_adm = QLineEdit(); self.borrow_adm.setPlaceholderText("Student Admission No.")
        self.borrow_bar = QLineEdit(); self.borrow_bar.setPlaceholderText("Barcode")
        btn_borrow = QPushButton("Borrow")
        btn_return = QPushButton("Return")
        top.addWidget(self.borrow_adm); top.addWidget(self.borrow_bar)
        top.addWidget(btn_borrow); top.addWidget(btn_return)
        l.addLayout(top)
        btn_borrow.clicked.connect(self.borrow_action)
        btn_return.clicked.connect(self.return_action)

        self.loan_table = QTableWidget(0,6)
        self.loan_table.setHorizontalHeaderLabels(["Barcode","Title","Student","Borrowed","Due","Condition"])
        self.loan_table.horizontalHeader().setStretchLastSection(True)
        l.addWidget(self.loan_table)
        return w

    def borrow_action(self):
        adm = self.borrow_adm.text().strip()
        bc = self.borrow_bar.text().strip()
        if not adm or not bc:
            self.success_msg.show_message("Enter admission and barcode", 3000)
            return
        student = self.db.find_student(self.school_id, adm)
        if not student:
            self.success_msg.show_message("Student not found", 3000)
            return
        book = self.db.find_book(self.school_id, bc)
        if not book:
            self.success_msg.show_message("Book not found", 3000)
            return
        if book["non_circulating"]:
            self.success_msg.show_message("Book not for borrowing", 3000)
            return
        ok, msg = self.db.borrow_book(self.school_id, book["id"], student["id"])
        if not ok:
            self.success_msg.show_message(msg, 3000)
        else:
            self.success_msg.show_message(f"Borrowed by {student['name']}")
            self.borrow_bar.clear()
            self.refresh_borrow_lists()

    def return_action(self):
        bc = self.borrow_bar.text().strip()
        if not bc:
            self.success_msg.show_message("Enter barcode", 3000)
            return
        book = self.db.find_book(self.school_id, bc)
        if not book:
            self.success_msg.show_message("Book not found", 3000)
            return
        ok, msg = self.db.return_book(self.school_id, book["id"])
        if not ok:
            self.success_msg.show_message(msg, 3000)
        else:
            self.success_msg.show_message(msg, 4000)
            self.borrow_bar.clear()
            self.refresh_borrow_lists()

    def refresh_borrow_lists(self):
        rows = self.db.current_loans(self.school_id)
        self.loan_table.setRowCount(0)
        for r in rows:
            row = self.loan_table.rowCount()
            self.loan_table.insertRow(row)
            self.loan_table.setItem(row,0,QTableWidgetItem(r["barcode"]))
            self.loan_table.setItem(row,1,QTableWidgetItem(r["title"]))
            self.loan_table.setItem(row,2,QTableWidgetItem(r["admission_no"]))
            self.loan_table.setItem(row,3,QTableWidgetItem(r["borrowed_at"][:10]))
            self.loan_table.setItem(row,4,QTableWidgetItem(r["due_date"][:10]))
            self.loan_table.setItem(row,5,QTableWidgetItem(r.get("condition", "")))

    def build_loans_page(self):
        w = QWidget(); l = QVBoxLayout(); w.setLayout(l)
        self.loans_table = QTableWidget(0,7)
        self.loans_table.setHorizontalHeaderLabels(["Barcode","Title","Adm","Name","Borrowed","Due","Condition"])
        self.loans_table.horizontalHeader().setStretchLastSection(True)
        l.addWidget(self.loans_table)
        return w

    def refresh_loans(self):
        rows = self.db.current_loans(self.school_id)
        self.loans_table.setRowCount(0)
        for r in rows:
            row = self.loans_table.rowCount()
            self.loans_table.insertRow(row)
            self.loans_table.setItem(row,0,QTableWidgetItem(r["barcode"]))
            self.loans_table.setItem(row,1,QTableWidgetItem(r["title"]))
            self.loans_table.setItem(row,2,QTableWidgetItem(r["admission_no"]))
            self.loans_table.setItem(row,3,QTableWidgetItem(r["student_name"]))
            self.loans_table.setItem(row,4,QTableWidgetItem(r["borrowed_at"][:10]))
            self.loans_table.setItem(row,5,QTableWidgetItem(r["due_date"][:10]))
            self.loans_table.setItem(row,6,QTableWidgetItem(r.get("condition", "")))

    def build_history_page(self):
        w = QWidget(); l = QVBoxLayout(); w.setLayout(l)
        self.hist_table = QTableWidget(0,7)
        self.hist_table.setHorizontalHeaderLabels(["Barcode","Title","Adm","Name","Borrowed","Due","Returned"])
        self.hist_table.horizontalHeader().setStretchLastSection(True)
        l.addWidget(self.hist_table)
        return w

    def refresh_history(self):
        rows = self.db.loan_history(self.school_id)
        self.hist_table.setRowCount(0)
        for r in rows:
            row = self.hist_table.rowCount()
            self.hist_table.insertRow(row)
            self.hist_table.setItem(row,0,QTableWidgetItem(r["barcode"]))
            self.hist_table.setItem(row,1,QTableWidgetItem(r["title"]))
            self.hist_table.setItem(row,2,QTableWidgetItem(r["admission_no"]))
            self.hist_table.setItem(row,3,QTableWidgetItem(r["student_name"]))
            self.hist_table.setItem(row,4,QTableWidgetItem(r["borrowed_at"][:10]))
            self.hist_table.setItem(row,5,QTableWidgetItem(r["due_date"][:10]))
            self.hist_table.setItem(row,6,QTableWidgetItem(r["returned_at"][:10] if r["returned_at"] else ""))

    def build_settings_page(self):
        w = QWidget(); l = QVBoxLayout(); w.setLayout(l)
        form = QFormLayout()
        self.spin_fine = QSpinBox(); self.spin_fine.setRange(0,10000)
        self.spin_days = QSpinBox(); self.spin_days.setRange(1,365)
        form.addRow("Fine per day:", self.spin_fine)
        form.addRow("Default loan days:", self.spin_days)
        btn_save = QPushButton("Save Settings")
        btn_save.clicked.connect(self.save_settings)
        
        btn_manage_users = QPushButton("Manage Users")
        btn_manage_users.clicked.connect(self.open_user_manager)
        l.addLayout(form)
        l.addWidget(btn_save)
        
        self.license_label = QLabel("")
        l.addWidget(self.license_label)
        
        if self.user["role"] != "Admin":
            btn_save.setDisabled(True)
        l.addWidget(btn_manage_users)
        l.addStretch()
        return w

    def load_settings(self):
        school = self.db.get_school_by_id(self.school_id)
        if school:
            self.school = school
            self.spin_fine.setValue(self.school["fine_per_day"])
            self.spin_days.setValue(self.school["default_loan_days"])
        
        if os.path.exists(LICENSE_FILE):
            ok, msg = validate_license_file(LICENSE_FILE, self.school["name"])
            self.license_label.setText(f"License: {'Valid' if ok else 'Invalid'}")
        else:
            self.license_label.setText("License: Not activated")

    def save_settings(self):
        if self.user["role"] != "Admin":
            self.success_msg.show_message("Only Admin can change settings", 3000)
            return
        fine = self.spin_fine.value()
        days = self.spin_days.value()
        self.db.update_school_settings(self.school_id, fine, days)
        self.success_msg.show_message("Settings updated!")
        self.load_settings()

    def open_user_manager(self):
        dlg = UserManagerDialog(self.db, self.school_id)
        dlg.exec_()

    def build_backup_page(self):
        w = QWidget(); l = QVBoxLayout(); w.setLayout(l)
        l.addWidget(QLabel("Database Backup and Restore"))
        btn_export = QPushButton("Export Database")
        btn_import = QPushButton("Import Database")
        btn_export.clicked.connect(self.export_db)
        btn_import.clicked.connect(self.import_db)
        l.addWidget(btn_export); l.addWidget(btn_import)
        l.addStretch()
        return w

    def export_db(self):
        path, _ = QFileDialog.getSaveFileName(self,"Export database", os.path.join(DB_DIR, "ellvins_backup.db"), "SQLite DB (*.db)")
        if not path:
            return
        try:
            self.db.conn.commit()
            shutil.copyfile(self.db.path, path)
            self.success_msg.show_message(f"Database exported")
        except Exception as e:
            self.success_msg.show_message(f"Export failed: {e}", 4000)

    def import_db(self):
        path, _ = QFileDialog.getOpenFileName(self,"Select database", "", "SQLite DB (*.db);;All files (*)")
        if not path:
            return
        confirm = QMessageBox.question(self,"Confirm", "This will replace current database. Continue?")
        if confirm != QMessageBox.Yes:
            return
        try:
            self.db.conn.close()
            shutil.copyfile(path, self.db.path)
            QMessageBox.information(self,"Restored","Database restored. Please restart.")
            self.close()
        except Exception as e:
            self.success_msg.show_message(f"Import failed: {e}", 4000)

class UserManagerDialog(QDialog):
    def __init__(self, db, school_id):
        super().__init__()
        self.db = db
        self.school_id = school_id
        self.setWindowTitle("Manage Users")
        self.setFixedSize(600,400)
        l = QVBoxLayout()
        self.table = QTableWidget(0,3)
        self.table.setHorizontalHeaderLabels(["Username","Role","Delete"])
        l.addWidget(self.table)
        self.refresh()
        
        form = QHBoxLayout()
        self.u_name = QLineEdit(); self.u_name.setPlaceholderText("username")
        self.u_pw = QLineEdit(); self.u_pw.setPlaceholderText("password"); self.u_pw.setEchoMode(QLineEdit.Password)
        self.u_role = QComboBox(); self.u_role.addItems(["Admin","Librarian","Assistant"])
        btn_add = QPushButton("Add user"); btn_add.clicked.connect(self.add_user)
        form.addWidget(self.u_name); form.addWidget(self.u_pw); form.addWidget(self.u_role); form.addWidget(btn_add)
        l.addLayout(form)
        self.setLayout(l)

    def refresh(self):
        rows = self.db.list_users(self.school_id)
        self.table.setRowCount(0)
        for r in rows:
            row = self.table.rowCount()
            self.table.insertRow(row)
            self.table.setItem(row,0,QTableWidgetItem(r["username"]))
            self.table.setItem(row,1,QTableWidgetItem(r["role"]))
            btn = QPushButton("Delete")
            btn.clicked.connect(lambda _,u=r["username"]: self.delete_user(u))
            self.table.setCellWidget(row,2,btn)

    def add_user(self):
        username = self.u_name.text().strip()
        pw = self.u_pw.text().strip()
        role = self.u_role.currentText()
        if not username or not pw:
            QMessageBox.warning(self,"Input","Username & password required")
            return
        ok = self.db.add_user(self.school_id, username, pw, role)
        if not ok:
            QMessageBox.warning(self,"Error","User exists")
        else:
            QMessageBox.information(self,"Success",f"User {username} created")
            self.u_name.clear()
            self.u_pw.clear()
            self.refresh()

    def delete_user(self, username):
        confirm = QMessageBox.question(self,"Confirm", f"Delete user {username}?")
        if confirm != QMessageBox.Yes:
            return
        self.db._exec("DELETE FROM users WHERE school_id=? AND username=?", (self.school_id, username))
        self.refresh()

def main():
    try:
        print("Initializing ELLVINS Library System...")
        db = DB(APP_DB)
        print("Database ready")
        
        app = QApplication(sys.argv)
        app.setApplicationName("ELLVINS Library System")
        app.setOrganizationName("ELLVINS")
        print("Qt Application initialized")
        
        login = LoginDialog(db)
        print("Login dialog created")
        
        if login.exec_() != QDialog.Accepted:
            print("Login cancelled")
            return
        
        school_row = login.school_row
        user_row = login.user
        
        if not school_row or not user_row:
            QMessageBox.warning(None, "Error", "Login information incomplete")
            return
        
        print(f"Login successful: {user_row['username']} at {school_row['name']}")
        mw = MainWindow(db, school_row, user_row)
        mw.show()
        print("Main window displayed")
        sys.exit(app.exec_())
        
    except Exception as e:
        print(f"FATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        input("Press Enter to exit...")
        sys.exit(1)

if __name__ == "__main__":
    main()