import tkinter as tk
from tkinter import ttk, messagebox, simpledialog
import tkinter.font as tkFont
import pyodbc
import math
from typing import List, Dict, Any, Optional


class SQLServerCRUDApp:
    def __init__(self, root):
        self.root = root
        self.root.title("SQL Server CRUD Application")
        self.root.geometry("800x600")

        # Database connection variables
        self.connection = None
        self.current_database = None
        self.current_schema = None  # Add schema variable
        self.current_table = None
        self.current_columns = []
        self.current_data = []
        self.filtered_data = []

        # Pagination variables
        self.page_size = 100
        self.current_page = 1
        self.total_pages = 1

        # Create GUI
        self.create_widgets()
        self.connect_to_server()

    def create_widgets(self):
        # Main frame
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # Configure grid weights
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)
        main_frame.rowconfigure(5, weight=1)  # Updated row index

        # Connection frame
        conn_frame = ttk.LabelFrame(main_frame, text="Connection", padding="5")
        conn_frame.grid(
            row=0, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=(0, 10)
        )
        conn_frame.columnconfigure(1, weight=1)

        ttk.Button(
            conn_frame, text="Connect to Server", command=self.connect_to_server
        ).grid(row=0, column=0, padx=(0, 10))

        self.connection_status = ttk.Label(conn_frame, text="Not connected")
        self.connection_status.grid(row=0, column=1, sticky=tk.W)

        # Database and Schema selection frame
        selection_frame = ttk.LabelFrame(
            main_frame, text="Database, Schema & Table Selection", padding="5"
        )
        selection_frame.grid(
            row=1, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=(0, 10)
        )
        selection_frame.columnconfigure(1, weight=1)
        selection_frame.columnconfigure(3, weight=1)
        selection_frame.columnconfigure(5, weight=1)

        ttk.Label(selection_frame, text="Database:").grid(row=0, column=0, padx=(0, 5))
        self.database_combo = ttk.Combobox(selection_frame, state="readonly")
        self.database_combo.grid(row=0, column=1, sticky=(tk.W, tk.E), padx=(0, 10))
        self.database_combo.bind("<<ComboboxSelected>>", self.on_database_selected)

        ttk.Label(selection_frame, text="Schema:").grid(row=0, column=2, padx=(0, 5))
        self.schema_combo = ttk.Combobox(selection_frame, state="readonly")
        self.schema_combo.grid(row=0, column=3, sticky=(tk.W, tk.E), padx=(0, 10))
        self.schema_combo.bind("<<ComboboxSelected>>", self.on_schema_selected)

        ttk.Label(selection_frame, text="Table/View:").grid(
            row=0, column=4, padx=(0, 5)
        )
        self.table_combo = ttk.Combobox(selection_frame, state="readonly")
        self.table_combo.grid(row=0, column=5, sticky=(tk.W, tk.E))
        self.table_combo.bind("<<ComboboxSelected>>", self.on_table_selected)

        # Filter frame
        filter_frame = ttk.LabelFrame(main_frame, text="Filter", padding="5")
        filter_frame.grid(
            row=2, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=(0, 10)
        )
        filter_frame.columnconfigure(1, weight=1)
        filter_frame.columnconfigure(3, weight=1)

        ttk.Label(filter_frame, text="Column:").grid(row=0, column=0, padx=(0, 5))
        self.filter_column_combo = ttk.Combobox(filter_frame, state="readonly")
        self.filter_column_combo.grid(
            row=0, column=1, sticky=(tk.W, tk.E), padx=(0, 10)
        )

        ttk.Label(filter_frame, text="Value:").grid(row=0, column=2, padx=(0, 5))
        self.filter_entry = ttk.Entry(filter_frame)
        self.filter_entry.grid(row=0, column=3, sticky=(tk.W, tk.E), padx=(0, 10))
        self.filter_entry.bind("<Return>", lambda e: self.apply_filter())

        ttk.Button(filter_frame, text="Apply Filter", command=self.apply_filter).grid(
            row=0, column=4, padx=(0, 10)
        )
        ttk.Button(filter_frame, text="Clear Filter", command=self.clear_filter).grid(
            row=0, column=5
        )
        ttk.Button(
            filter_frame,
            text="Advanced Filter",
            command=self.open_advanced_filter_dialog,
        ).grid(row=0, column=6, padx=(10, 0))

        # CRUD buttons frame
        crud_frame = ttk.Frame(main_frame)
        crud_frame.grid(
            row=3, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=(0, 10)
        )

        ttk.Button(crud_frame, text="Refresh", command=self.refresh_data).pack(
            side=tk.LEFT, padx=(0, 5)
        )
        ttk.Button(crud_frame, text="Add Record", command=self.add_record).pack(
            side=tk.LEFT, padx=(0, 5)
        )
        ttk.Button(crud_frame, text="Edit Record", command=self.edit_record).pack(
            side=tk.LEFT, padx=(0, 5)
        )
        ttk.Button(crud_frame, text="Delete Record", command=self.delete_record).pack(
            side=tk.LEFT, padx=(0, 5)
        )

        # Data display frame
        data_frame = ttk.LabelFrame(main_frame, text="Data", padding="5")
        data_frame.grid(row=4, column=0, columnspan=3, sticky=(tk.W, tk.E, tk.N, tk.S))
        data_frame.columnconfigure(0, weight=1)
        data_frame.rowconfigure(0, weight=1)

        # Treeview with scrollbars
        tree_frame = ttk.Frame(data_frame)
        tree_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        tree_frame.columnconfigure(0, weight=1)
        tree_frame.rowconfigure(0, weight=1)

        self.tree = ttk.Treeview(tree_frame)
        self.tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # Scrollbars
        v_scrollbar = ttk.Scrollbar(
            tree_frame, orient=tk.VERTICAL, command=self.tree.yview
        )
        v_scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))
        self.tree.configure(yscrollcommand=v_scrollbar.set)

        h_scrollbar = ttk.Scrollbar(
            tree_frame, orient=tk.HORIZONTAL, command=self.tree.xview
        )
        h_scrollbar.grid(row=1, column=0, sticky=(tk.W, tk.E))
        self.tree.configure(xscrollcommand=h_scrollbar.set)

        # Pagination frame
        pagination_frame = ttk.Frame(data_frame)
        pagination_frame.grid(row=1, column=0, pady=(10, 0))

        self.prev_button = ttk.Button(
            pagination_frame, text="Previous", command=self.prev_page, state="disabled"
        )
        self.prev_button.pack(side=tk.LEFT, padx=(0, 5))

        self.page_label = ttk.Label(pagination_frame, text="Page 1 of 1")
        self.page_label.pack(side=tk.LEFT, padx=(0, 5))

        self.next_button = ttk.Button(
            pagination_frame, text="Next", command=self.next_page, state="disabled"
        )
        self.next_button.pack(side=tk.LEFT, padx=(0, 5))

        self.records_label = ttk.Label(pagination_frame, text="0 records")
        self.records_label.pack(side=tk.LEFT, padx=(10, 0))

    def load_databases(self):
        """Load available databases"""
        if not self.connection:
            return

        try:
            cursor = self.connection.cursor()
            cursor.execute(
                "SELECT name FROM sys.databases WHERE database_id > 4 ORDER BY name"
            )
            databases = [row[0] for row in cursor.fetchall()]
            self.database_combo["values"] = databases
            if databases:
                self.database_combo.current(0)
                self.on_database_selected(None)
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load databases:\n{str(e)}")

    def on_database_selected(self, event):
        """Handle database selection"""
        selected_db = self.database_combo.get()
        if not selected_db:
            return

        self.current_database = selected_db
        self.load_schemas()

    def load_schemas(self):
        """Load available schemas from selected database"""
        if not self.connection or not self.current_database:
            return

        try:
            cursor = self.connection.cursor()
            cursor.execute(f"USE [{self.current_database}]")

            # Get schemas
            cursor.execute(
                """
                SELECT SCHEMA_NAME 
                FROM INFORMATION_SCHEMA.SCHEMATA 
                ORDER BY SCHEMA_NAME
            """
            )
            schemas = [
                row[0] for row in cursor.fetchall() if not row[0].startswith("db_")
            ]

            remove_schemas: list[str] = ["guest", "INFORMATION_SCHEMA", "sys"]
            for idx, value in enumerate(schemas):
                if value in remove_schemas:
                    schemas.pop(idx)

            self.schema_combo["values"] = schemas
            if schemas:
                # Try to default to 'dbo' if it exists, otherwise first schema
                if "dbo" in schemas:
                    self.schema_combo.set("dbo")
                else:
                    self.schema_combo.current(0)
                self.on_schema_selected(None)

        except Exception as e:
            messagebox.showerror("Error", f"Failed to load schemas:\n{str(e)}")

    def on_schema_selected(self, event):
        """Handle schema selection"""
        selected_schema = self.schema_combo.get()
        if not selected_schema:
            return

        self.current_schema = selected_schema
        self.load_tables()

    def load_tables(self):
        """Load tables and views from selected database and schema"""
        if not self.connection or not self.current_database or not self.current_schema:
            return

        try:
            cursor = self.connection.cursor()
            cursor.execute(f"USE [{self.current_database}]")

            # Get tables and views for the specific schema
            query = """
            SELECT TABLE_NAME, TABLE_TYPE 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE IN ('BASE TABLE', 'VIEW')
            AND TABLE_SCHEMA = ?
            ORDER BY TABLE_TYPE, TABLE_NAME
            """
            cursor.execute(query, self.current_schema)

            tables = []
            for row in cursor.fetchall():
                table_name, table_type = row
                display_name = f"{table_name} ({table_type})"
                tables.append(display_name)

            self.table_combo["values"] = tables
            if tables:
                self.table_combo.current(0)
                self.on_table_selected(None)

        except Exception as e:
            messagebox.showerror("Error", f"Failed to load tables:\n{str(e)}")

    def on_table_selected(self, event):
        """Handle table selection"""
        selected_item = self.table_combo.get()
        if not selected_item:
            return

        # Extract table name (remove type suffix)
        self.current_table = selected_item.split(" (")[0]
        self.load_table_structure()
        self.load_data()

    def load_table_structure(self):
        """Load table structure and populate filter column dropdown"""
        if (
            not self.connection
            or not self.current_database
            or not self.current_schema
            or not self.current_table
        ):
            return

        try:
            cursor = self.connection.cursor()
            cursor.execute(f"USE [{self.current_database}]")

            # Get column information for specific schema and table
            query = """
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = ? AND TABLE_SCHEMA = ?
            ORDER BY ORDINAL_POSITION
            """
            cursor.execute(query, self.current_table, self.current_schema)

            self.current_columns = []
            column_names = []

            for row in cursor.fetchall():
                column_info = {
                    "name": row[0],
                    "type": row[1],
                    "nullable": row[2] == "YES",
                    "default": row[3],
                }
                self.current_columns.append(column_info)
                column_names.append(row[0])

            # Update filter column dropdown
            self.filter_column_combo["values"] = column_names
            if column_names:
                self.filter_column_combo.current(0)

        except Exception as e:
            messagebox.showerror("Error", f"Failed to load table structure:\n{str(e)}")

    def load_data(self, where_clause=""):
        """Load data from current table with pagination"""
        if (
            not self.connection
            or not self.current_database
            or not self.current_schema
            or not self.current_table
        ):
            return

        try:
            cursor = self.connection.cursor()
            cursor.execute(f"USE [{self.current_database}]")

            # Use schema.table format for queries
            full_table_name = f"[{self.current_schema}].[{self.current_table}]"

            # Count total records
            count_query = f"SELECT COUNT(*) FROM {full_table_name}"
            if where_clause:
                count_query += f" WHERE {where_clause}"

            cursor.execute(count_query)
            total_records = cursor.fetchone()[0]

            # Calculate pagination
            self.total_pages = max(1, math.ceil(total_records / self.page_size))
            if self.current_page > self.total_pages:
                self.current_page = 1

            # Get data for current page
            offset = (self.current_page - 1) * self.page_size

            data_query = f"""
            SELECT * FROM {full_table_name}
            {f'WHERE {where_clause}' if where_clause else ''}
            ORDER BY (SELECT NULL)
            OFFSET {offset} ROWS
            FETCH NEXT {self.page_size} ROWS ONLY
            """

            cursor.execute(data_query)
            self.current_data = cursor.fetchall()

            # Update treeview
            self.update_treeview()
            self.update_pagination_controls(total_records)

        except Exception as e:
            messagebox.showerror("Error", f"Failed to load data:\n{str(e)}")

    def update_treeview(self):
        """Update the treeview with current data"""
        # Clear existing data
        for item in self.tree.get_children():
            self.tree.delete(item)

        if not self.current_columns or not self.current_data:
            return

        # Configure columns
        column_names = [col["name"] for col in self.current_columns]
        self.tree["columns"] = column_names
        self.tree["show"] = "headings"

        # Configure column headings and widths
        for col_name in column_names:
            self.tree.heading(col_name, text=col_name)
            self.tree.column(col_name, width=100, minwidth=50)

        # Insert data
        for row in self.current_data:
            # Convert None values to empty strings for display
            display_row = [str(val) if val is not None else "" for val in row]
            self.tree.insert("", "end", values=display_row)

        self.autosize_tree_columns()

    def autosize_tree_columns(self, padding=20):
        """Automatically resizes the columns in self.tree to fit the content."""
        style = ttk.Style()
        treeview_font = tkFont.nametofont(style.lookup("Treeview", "font"))

        for col in self.tree["columns"]:
            max_width = treeview_font.measure(col)
            for item in self.tree.get_children():
                cell_text = self.tree.set(item, col)
                cell_width = treeview_font.measure(cell_text)
                max_width = max(max_width, cell_width)
            self.tree.column(col, width=max_width + padding)

    def update_pagination_controls(self, total_records):
        """Update pagination controls"""
        self.page_label.config(text=f"Page {self.current_page} of {self.total_pages}")
        self.records_label.config(text=f"{total_records} records")

        self.prev_button.config(state="normal" if self.current_page > 1 else "disabled")
        self.next_button.config(
            state="normal" if self.current_page < self.total_pages else "disabled"
        )

    def prev_page(self):
        """Go to previous page"""
        if self.current_page > 1:
            self.current_page -= 1
            if hasattr(self, "current_filter") and self.current_filter:
                self.load_data(self.current_filter)
            else:
                self.load_data()

    def next_page(self):
        """Go to next page"""
        if self.current_page < self.total_pages:
            self.current_page += 1
            if hasattr(self, "current_filter") and self.current_filter:
                self.load_data(self.current_filter)
            else:
                self.load_data()

    def apply_filter(self):
        """Apply filter to data"""
        column = self.filter_column_combo.get()
        value = self.filter_entry.get().strip()

        if not column or not value:
            messagebox.showwarning(
                "Filter", "Please select a column and enter a filter value."
            )
            return

        try:
            # Simple LIKE filter - you can enhance this with more operators
            where_clause = f"[{column}] LIKE '%{value}%'"
            self.current_filter = where_clause
            self.current_page = 1  # Reset to first page
            self.load_data(where_clause)

        except Exception as e:
            messagebox.showerror("Filter Error", f"Failed to apply filter:\n{str(e)}")

    def clear_filter(self):
        """Clear current filter"""
        self.filter_entry.delete(0, tk.END)
        self.current_filter = ""
        self.current_page = 1
        self.load_data()

    def refresh_data(self):
        """Refresh current data"""
        if hasattr(self, "current_filter") and self.current_filter:
            self.load_data(self.current_filter)
        else:
            self.load_data()

    def get_selected_record(self):
        """Get currently selected record from treeview"""
        selection = self.tree.selection()
        if not selection:
            messagebox.showwarning("Selection", "Please select a record.")
            return None

        item = selection[0]
        values = self.tree.item(item, "values")
        return dict(zip([col["name"] for col in self.current_columns], values))

    def add_record(self):
        """Add new record"""
        if (
            not self.current_table
            or not self.current_columns
            or not self.current_schema
        ):
            messagebox.showwarning("Add Record", "Please select a table first.")
            return

        # Create dialog for new record
        dialog = RecordDialog(self.root, "Add Record", self.current_columns)
        if dialog.result:
            try:
                cursor = self.connection.cursor()
                cursor.execute(f"USE [{self.current_database}]")

                # Use schema.table format
                full_table_name = f"[{self.current_schema}].[{
                    self.current_table}]"

                # Build INSERT query
                columns = list(dialog.result.keys())
                placeholders = ", ".join(["?" for _ in columns])
                column_names = ", ".join([f"[{col}]" for col in columns])

                query = f"INSERT INTO {full_table_name} ({column_names}) VALUES ({
                    placeholders})"
                values = [
                    dialog.result[col] if dialog.result[col] != "" else None
                    for col in columns
                ]

                cursor.execute(query, values)
                self.connection.commit()

                messagebox.showinfo("Success", "Record added successfully.")
                self.refresh_data()

            except Exception as e:
                messagebox.showerror("Error", f"Failed to add record:\n{str(e)}")
                self.connection.rollback()

    def edit_record(self):
        """Edit selected record"""
        record = self.get_selected_record()
        if not record:
            return

        # Create dialog for editing record
        dialog = RecordDialog(self.root, "Edit Record", self.current_columns, record)
        if dialog.result:
            try:
                cursor = self.connection.cursor()
                cursor.execute(f"USE [{self.current_database}]")

                # Use schema.table format
                full_table_name = f"[{self.current_schema}].[{
                    self.current_table}]"

                # Build UPDATE query (assuming first column is primary key)
                pk_column = self.current_columns[0]["name"]
                pk_value = record[pk_column]

                set_clauses = []
                values = []

                for col_name, value in dialog.result.items():
                    if col_name != pk_column:  # Don't update primary key
                        set_clauses.append(f"[{col_name}] = ?")
                        values.append(value if value != "" else None)

                if not set_clauses:
                    messagebox.showwarning("Edit", "No changes to save.")
                    return

                query = f"UPDATE {full_table_name} SET {
                    ', '.join(set_clauses)} WHERE [{pk_column}] = ?"
                values.append(pk_value)

                cursor.execute(query, values)
                self.connection.commit()

                messagebox.showinfo("Success", "Record updated successfully.")
                self.refresh_data()

            except Exception as e:
                messagebox.showerror("Error", f"Failed to update record:\n{str(e)}")
                self.connection.rollback()

    def connect_to_server(self):
        """Connect to SQL Server"""
        try:
            # Connection dialog
            server = simpledialog.askstring("Server Connection", "Enter server name:")
            if not server:
                return

            username = simpledialog.askstring(
                "Server Connection", "Enter username (leave empty for Windows Auth):"
            )
            password = None

            if username:
                password = simpledialog.askstring(
                    "Server Connection", "Enter password:", show="*"
                )
                conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={
                    server};UID={username};PWD={password}"
            else:
                conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={
                    server};Trusted_Connection=yes"
            self.connection = pyodbc.connect(conn_str)
            self.connection_status.config(text=f"Connected to {server}")
            self.load_databases()

        except Exception as e:
            messagebox.showerror(
                "Connection Error", f"Failed to connect to server:\n{str(e)}"
            )
            self.connection_status.config(text="Connection failed")

    def delete_record(self):
        """Delete selected record"""
        record = self.get_selected_record()
        if not record:
            return

        # Confirm deletion
        if not messagebox.askyesno(
            "Confirm Delete", "Are you sure you want to delete this record?"
        ):
            return

        try:
            cursor = self.connection.cursor()
            cursor.execute(f"USE [{self.current_database}]")

            # Use schema.table format
            full_table_name = f"[{self.current_schema}].[{self.current_table}]"

            # Build DELETE query (assuming first column is primary key)
            pk_column = self.current_columns[0]["name"]
            pk_value = record[pk_column]

            query = f"DELETE FROM {full_table_name} WHERE [{pk_column}] = ?"
            cursor.execute(query, pk_value)
            self.connection.commit()

            messagebox.showinfo("Success", "Record deleted successfully.")
            self.refresh_data()

        except Exception as e:
            messagebox.showerror("Error", f"Failed to delete record:\n{str(e)}")
            self.connection.rollback()

    def open_advanced_filter_dialog(self):
        if not self.current_columns:
            messagebox.showwarning("Advanced Filter", "No table selected.")
            return

        dialog = AdvancedFilterDialog(self.root, self.current_columns)
        if dialog.result:
            # Build WHERE clause from result
            filters = []
            for col, op, val in dialog.result:
                if val.strip() == "":
                    continue
                safe_col = f"[{col}]"
                if op.lower() == "like":
                    filters.append(f"{safe_col} LIKE '%{val}%'")
                else:
                    filters.append(f"{safe_col} {op} '{val}'")
            where_clause = " AND ".join(filters)
            if where_clause:
                self.current_filter = where_clause
                self.current_page = 1
                self.load_data(where_clause)


class RecordDialog:
    def __init__(self, parent, title, columns, initial_values=None):
        self.result = None

        # Create dialog window
        self.dialog = tk.Toplevel(parent)
        self.dialog.title(title)
        self.dialog.geometry("400x500")
        self.dialog.transient(parent)
        self.dialog.grab_set()

        # Center dialog
        self.dialog.geometry(
            "+%d+%d" % (parent.winfo_rootx() + 50, parent.winfo_rooty() + 50)
        )

        # Create form
        main_frame = ttk.Frame(self.dialog, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # Create scrollable frame for form fields
        canvas = tk.Canvas(main_frame)
        scrollbar = ttk.Scrollbar(main_frame, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)

        scrollable_frame.bind(
            "<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )

        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        # Form fields
        self.entries = {}
        for i, column in enumerate(columns):
            col_name = column["name"]
            col_type = column["type"]

            ttk.Label(scrollable_frame, text=f"{col_name} ({col_type}):").grid(
                row=i, column=0, sticky=tk.W, padx=(0, 10), pady=(0, 5)
            )

            entry = ttk.Entry(scrollable_frame, width=30)
            entry.grid(row=i, column=1, sticky=(tk.W, tk.E), pady=(0, 5))

            if initial_values and col_name in initial_values:
                entry.insert(0, str(initial_values[col_name]))

            self.entries[col_name] = entry

        scrollable_frame.columnconfigure(1, weight=1)

        # Buttons
        button_frame = ttk.Frame(self.dialog)
        button_frame.pack(fill=tk.X, padx=10, pady=10)

        ttk.Button(button_frame, text="Save", command=self.save).pack(
            side=tk.RIGHT, padx=(5, 0)
        )
        ttk.Button(button_frame, text="Cancel", command=self.cancel).pack(side=tk.RIGHT)

        # Handle window close
        self.dialog.protocol("WM_DELETE_WINDOW", self.cancel)

        # Wait for dialog to close
        self.dialog.wait_window()

    def save(self):
        """Save form data"""
        self.result = {}
        for col_name, entry in self.entries.items():
            self.result[col_name] = entry.get()
        self.dialog.destroy()

    def cancel(self):
        """Cancel dialog"""
        self.result = None
        self.dialog.destroy()


class AdvancedFilterDialog:
    def __init__(self, parent, columns):
        self.result = None
        self.dialog = tk.Toplevel(parent)
        self.dialog.title("Advanced Filter")
        self.dialog.geometry("500x600")
        self.dialog.transient(parent)
        self.dialog.grab_set()

        frame = ttk.Frame(self.dialog, padding=10)
        frame.pack(fill=tk.BOTH, expand=True)

        canvas = tk.Canvas(frame)
        scrollbar = ttk.Scrollbar(frame, orient="vertical", command=canvas.yview)
        scroll_frame = ttk.Frame(canvas)

        scroll_frame.bind(
            "<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        canvas.create_window((0, 0), window=scroll_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        self.entries = []
        operators = ["=", "!=", ">", "<", ">=", "<=", "LIKE"]

        for i, col in enumerate(columns):
            ttk.Label(scroll_frame, text=col["name"]).grid(
                row=i, column=0, padx=5, pady=5, sticky=tk.W
            )

            op_cb = ttk.Combobox(
                scroll_frame, values=operators, state="readonly", width=5
            )
            op_cb.grid(row=i, column=1, padx=5, pady=5)
            op_cb.set("=")

            val_entry = ttk.Entry(scroll_frame, width=20)
            val_entry.grid(row=i, column=2, padx=5, pady=5)

            self.entries.append((col["name"], op_cb, val_entry))

        # Buttons
        btn_frame = ttk.Frame(self.dialog)
        btn_frame.pack(fill=tk.X, padx=10, pady=10)
        ttk.Button(btn_frame, text="Apply", command=self.apply).pack(
            side=tk.RIGHT, padx=5
        )
        ttk.Button(btn_frame, text="Cancel", command=self.cancel).pack(side=tk.RIGHT)

        self.dialog.protocol("WM_DELETE_WINDOW", self.cancel)
        self.dialog.wait_window()

    def apply(self):
        self.result = []
        for col, op_cb, val_entry in self.entries:
            op = op_cb.get()
            val = val_entry.get()
            if val.strip():
                self.result.append((col, op, val))
        self.dialog.destroy()

    def cancel(self):
        self.result = None
        self.dialog.destroy()


if __name__ == "__main__":
    root = tk.Tk()
    app = SQLServerCRUDApp(root)
    root.mainloop()
