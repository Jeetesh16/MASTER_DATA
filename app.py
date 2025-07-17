import pandas as pd
import streamlit as st
import datetime
from snowflake_connector import get_snowflake_session


class SnowflakeDataApp:
    def __init__(self):
        st.set_page_config(page_title="Snowflake Data App", layout="wide")
        self.session = get_snowflake_session()
        self._initialize_session_state()
        self.selected_db = st.session_state.get("selected_db")
        self.selected_table = st.session_state.get("selected_table")
        self.selected_schema = st.session_state.get("selected_schema")

    def _initialize_session_state(self):
        for key, default in {
            "selected_db":None,
            "selected_table": None,
            "selected_schema": None,
            "username": "User",  # Default username
            "data_entry": pd.DataFrame(),
            "pagination_offset": 0,
            "new_data": pd.DataFrame(),
            'filter_values': {}
        }.items():
            if key not in st.session_state:
                st.session_state[key] = default

    def _sidebar(self):
        st.markdown(
            """
            <style>
                .truncate-box {
                    display: inline-block;
                    min-width:700px;
                    max-width: 1520px;
                    white-space: nowrap;
                    overflow: hidden;
                    text-overflow: ellipsis;
                    vertical-align: middle;
                    font-weight: bold;
                    font-size: 8px;
                }
                div[data-testid="stSidebar"] button,
                div[data-testid="stButton"] button {
                    border: none !important;
                    outline: none !important;
                    box-shadow: none !important;
                    background-color: transparent !important;
                    margin: 0 !important;
                    padding: 8px 16px !important;
                }
                div[data-testid="stSidebar"] .stButton,
                div[data-testid="stSidebar"] .stMarkdown,
                div[data-testid="stSidebar"] .stSubheader,
                div[data-testid="stSidebar"] .stTextInput,
                div[data-testid="stSidebar"] .stSelectbox,
                div[data-testid="stSidebar"] .stMultiselect {
                    margin-bottom: 2px !important;
                    padding: 0 !important;
                }
                div[data-testid="stSidebar"] .stMarkdown,
                div[data-testid="stSidebar"] .stSubheader {
                    margin-top: 0 !important;
                    margin-bottom: 5px !important;
                }
                div[data-testid="stSidebar"] button:hover,
                div[data-testid="stButton"] button:hover {
                    background-color: transparent !important;
                    box-shadow: none !important;
                }
                div[data-testid="stSidebar"] button:focus,
                div[data-testid="stButton"] button:focus {
                    outline: none !important;
                }
                div[data-testid="stSidebar"] .stTextInput,
                div[data-testid="stSidebar"] .stSelectbox,
                div[data-testid="stSidebar"] .stMultiselect {
                    padding: 0 !important;
                    margin: 0 !important;
                }
            </style>
            """,
            unsafe_allow_html=True,
        )


        with st.sidebar:
            st.markdown("---")
            st.subheader("📁 Databases, Schemas and Tables")

            try:
                # 🔹 Step 1: Get list of databases
                db_results = self.session.sql("SHOW DATABASES").collect()
                database_list = [db["name"] for db in db_results]

                # 🔹 Step 2: Show databases as buttons
                for db_name in database_list:
                    is_selected_db = db_name == st.session_state.get("selected_db")

                    if st.button(f"🗂️ {db_name}", key=f"db_{db_name}"):
                        if is_selected_db:
                            st.session_state["selected_db"] = None
                            st.session_state["selected_schema"] = None
                            st.session_state["selected_table"] = None
                        else:
                            st.session_state["selected_db"] = db_name
                            st.session_state["selected_schema"] = None
                            st.session_state["selected_table"] = None
                        st.rerun()

                    # 🔹 Step 3: If DB is selected, show schemas
                    if is_selected_db:
                        try:
                            schemas_query = [
                                s["SCHEMA_NAME"]
                                for s in self.session.sql(
                                    f"SELECT schema_name FROM {db_name}.information_schema.schemata WHERE schema_name <> 'INFORMATION_SCHEMA'"
                                ).collect()
                            ]

                            for schema_name in schemas_query:
                                is_selected_schema = schema_name == st.session_state.get("selected_schema")

                                if st.button(f"📦 {schema_name}", key=f"schema_{db_name}_{schema_name}"):
                                    if is_selected_schema:
                                        st.session_state["selected_schema"] = None
                                        st.session_state["selected_table"] = None
                                    else:
                                        st.session_state["selected_schema"] = schema_name
                                        st.session_state["selected_table"] = None
                                    st.rerun()

                                # 🔹 Step 4: If schema is selected, show tables
                                if is_selected_schema:
                                    try:
                                        tables = self.session.sql(f"SHOW TABLES IN {db_name}.{schema_name}").collect()
                                        for table_info in tables:
                                            table_name = table_info["name"]
                                            if st.button(
                                                f"📄 {table_name}",
                                                key=f"table_{db_name}_{schema_name}_{table_name}",
                                            ):
                                                st.session_state["selected_table"] = table_name
                                                st.rerun()
                                    except Exception as e:
                                        st.error(f"Could not load tables for schema {schema_name}: {e}")

                        except Exception as e:
                            st.error(f"Could not load schemas for database {db_name}: {e}")

            except Exception as e:
                st.error(f"Could not load databases: {e}")

            


    def _validate_row(self, row, column_metadata, table_name,schema_name,database_name):
        errors = []
        # --- Field-level validation ---
        for col_meta in column_metadata:
            col_name = col_meta['COLUMN_NAME']
            data_type = col_meta['DATA_TYPE']
            is_nullable = col_meta['IS_NULLABLE'] == 'YES'
            value = row.get(col_name)

            # Not Null check
            if not is_nullable and (pd.isna(value) or str(value).strip() == ""):
                errors.append(f"{col_name} cannot be NULL.")

            # Data Type Validation
            if pd.notna(value):
                if data_type == 'DATE' or data_type == 'TIMESTAMP':
                    try:
                        # Date format check
                        datetime.datetime.strptime(str(value), '%Y-%m-%d')
                    except ValueError:
                        errors.append(f"{col_name} must be in %Y-%m-%d format.")
                elif data_type == 'INTEGER':
                    try:
                        int(value)
                    except ValueError:
                        errors.append(f"{col_name} must be an INTEGER.")
                elif data_type == 'FLOAT':
                    try:
                        float(value)
                    except ValueError:
                        errors.append(f"{col_name} must be a FLOAT.")
                elif data_type == 'STRING':
                    if not isinstance(value, str):
                        errors.append(f"{col_name} must be a STRING.")
                # Add more data types if necessary, e.g., BOOLEAN, DECIMAL, etc.

        # --- Full-row duplicate check (optional, separate from PK check) ---
        where_clauses = []
        for col_meta in column_metadata:
            col_name = col_meta['COLUMN_NAME']
            value = row.get(col_name)
            if pd.notna(value):
                where_clauses.append(f"{col_name} = '{value}'")
            else:
                where_clauses.append(f"{col_name} IS NULL")

        if where_clauses:
            where_clause = " AND ".join(where_clauses)
            dup_query = f"SELECT COUNT(*) AS COUNT FROM {database_name}.{schema_name}.{table_name} WHERE {where_clause}"
            result = self.session.sql(dup_query).collect()[0]['COUNT']
            if result > 0:
                errors.append(f"Duplicate row found: {row.to_dict()}")

        return errors

    def _get_distinct_column_values(self, schema, table, column,database_name):
        try:
            query = f"""
                SELECT DISTINCT {column}
                FROM {database_name}.{schema}.{table}
                WHERE {column} IS NOT NULL
                ORDER BY 1
            """
            results = self.session.sql(query).collect()
            return [row[column] for row in results]
        except Exception as e:
            st.error(f"Error fetching values for {column}: {e}")
            return []

    def _view_data_with_pagination(self):
        if self.selected_table is None:
            st.warning("Please select a table first from the sidebar.")
            return

        offset = st.session_state.get("pagination_offset", 0)
        limit = 50

        try:
            col_query = f"""
                SELECT column_name
                FROM {self.selected_db}.information_schema.columns
                WHERE table_name = UPPER('{self.selected_table}')
                AND table_schema = UPPER('{self.selected_schema}')
                ORDER BY ordinal_position
            """
            column_names = [col["COLUMN_NAME"] for col in self.session.sql(col_query).collect()]
        except Exception as e:
            st.error(f"Error fetching column names: {e}")
            return

        # Fetch filter values from session_state
        filter_values = st.session_state['filter_values']
        filter_applied = False  # Flag to check if filter is applied
        col1, _ = st.columns([1, 3])
        with col1:
            selected_filter_columns = st.multiselect(
                "Filters:",
                options=column_names#,help="Choose columns to filter. Each one will display a dropdown of distinct values."
            )

        if selected_filter_columns:
            filter_cols = st.columns(len(selected_filter_columns))
            for idx, col_name in enumerate(selected_filter_columns):
                with filter_cols[idx]:
                    distinct_vals = self._get_distinct_column_values(self.selected_schema, self.selected_table, col_name,self.selected_db)
                    selected_val = st.selectbox(
                        label="",
                        options=[col_name] + [str(v) for v in distinct_vals],
                        index=0,
                        key=f"filter_{col_name}",
                        placeholder=col_name
                    )
                    if selected_val != col_name:
                        filter_values[col_name] = selected_val
                        filter_applied = True  # Mark that a filter has been applied
                    else:
                        if col_name in filter_values:
                            del filter_values[col_name]

        st.session_state['filter_values'] = filter_values

        # Reset pagination to the first page when a filter is applied
        if filter_applied:
            st.session_state["pagination_offset"] = 0  # Reset pagination offset to 0 when filter is applied

        where_clauses = [f"{col} = '{val}'" for col, val in filter_values.items()]
        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

        try:
            data_query = f"""
                SELECT * FROM {self.selected_db}.{self.selected_schema}.{self.selected_table}
                {where_sql}
                LIMIT {limit} OFFSET {offset}
            """
            data = self.session.sql(data_query).collect()
            df = pd.DataFrame(data, columns=column_names)
        except Exception as e:
            st.error(f"Error fetching data: {e}")
            return

        # ✅ Add checkbox column for deletion at the start
        df["✅ Delete"] = False
        df = df[["✅ Delete"] + column_names]

        # Add empty row
        empty_row = pd.DataFrame([[False] + [None] * len(column_names)], columns=["✅ Delete"] + column_names)
        df_with_empty_row = pd.concat([df, empty_row], ignore_index=True).reset_index(drop=True)

        st.markdown(
            """
            <style>
            div[data-testid="stDataFrame"] button,
            div[data-testid="stDataEditor"] button {
                display: none !important;
            }
            </style>
            """,
            unsafe_allow_html=True,
        )

        st.markdown("### 📝 Data Editor ")
        edited_df = st.data_editor(
            df_with_empty_row,
            use_container_width=True,
            num_rows="fixed",
            key=f"editor_{self.selected_table}_{offset}"
        )

        # Fetch metadata
        column_metadata_query = f"""
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
            FROM {self.selected_db}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{self.selected_table}' AND TABLE_SCHEMA = '{st.session_state["selected_schema"]}'
        """
        column_metadata = self.session.sql(column_metadata_query).collect()

        # Save changes
        if st.button("💾 Save Changes"):
            changes = 0
            for i, row in edited_df.iterrows():
                is_new_row = i >= len(df)
                row_data = row[column_names]
                has_data = row_data.notna().any()

                if is_new_row and has_data:
                    errors = self._validate_row(row_data, column_metadata, f"{self.selected_table}",f"{self.selected_schema}",f"{self.selected_db}")
                    if errors:
                        st.error(f"Row {i + 1} validation errors: {', '.join(errors)}")
                    else:
                        cols = ", ".join(column_names)
                        vals = ", ".join([f"'{v}'" if pd.notna(v) else "NULL" for v in row_data])
                        insert_query = f"INSERT INTO {self.selected_db}.{self.selected_schema}.{self.selected_table} ({cols}) VALUES ({vals})"
                        self.session.sql(insert_query).collect()
                        changes += 1
                elif not is_new_row:
                    updates = []
                    for col in column_names:
                        orig_val = df.iloc[i][col]
                        new_val = row[col]
                        if pd.isna(orig_val) and pd.isna(new_val):
                            continue
                        if orig_val != new_val:
                            val = f"'{new_val}'" if pd.notna(new_val) else "NULL"
                            updates.append(f"{col} = {val}")

                    if updates:
                        where_key = f"{column_names[0]} = '{df.iloc[i][column_names[0]]}'"
                        update_query = f"""
                            UPDATE {self.selected_db}.{self.selected_schema}.{self.selected_table}
                            SET {', '.join(updates)}
                            WHERE {where_key}
                        """
                        try:
                            self.session.sql(update_query).collect()
                            changes += 1
                        except Exception as e:
                            st.error(f"Update failed for row {i+1}: {e}")

            if changes > 0:
                st.success(f"✅ {changes} changes saved successfully.")
                st.cache_data.clear()  # Ensure we clear the cache when saving changes
                st.rerun()
            else:
                st.info("No changes detected.")

        # ✅ Multi-row Deletion
        rows_to_delete = edited_df[edited_df["✅ Delete"] == True]
        if not rows_to_delete.empty:
            st.warning(f"Selected {len(rows_to_delete)} row(s) for deletion.")
            if st.button("🗑️ Delete Selected Rows"):
                deleted = 0
                for _, row in rows_to_delete.iterrows():
                    try:
                        pk_col = column_names[0]
                        pk_val = row[pk_col]
                        if pd.notna(pk_val):
                            delete_query = f"""
                                DELETE FROM {self.selected_db}.{self.selected_schema}.{self.selected_table}
                                WHERE {pk_col} = '{pk_val}'
                            """
                            self.session.sql(delete_query).collect()
                            deleted += 1
                    except Exception as e:
                        st.error(f"Delete failed for PK={pk_val}: {e}")
                if deleted > 0:
                    st.success(f"✅ {deleted} row(s) deleted.")
                    st.cache_data.clear()
                    st.rerun()

        # Pagination
        st.markdown("---")
        col1, col2, _ = st.columns([1, 1, 8])
        with col1:
            if st.button("⬅️ Prev") and offset > 0:
                st.session_state["pagination_offset"] -= limit
                st.rerun()
        with col2:
            if st.button("➡️ Next") and len(df) == limit:
                st.session_state["pagination_offset"] += limit
                st.rerun()

    def _upload_file(self):
        st.subheader("📤 Upload Data from File")
        selected_table = st.session_state.get("selected_table")

        file = st.file_uploader("Upload CSV or Excel file", type=["csv", "xlsx"])

        if not selected_table:
            st.warning("Please select a table first.")
            return

        if file:
            try:
                if file.name.endswith(".csv"):
                    df = pd.read_csv(file)
                    df.columns =df.columns = df.columns.str.strip().str.upper()
                    st.dataframe(df, use_container_width=True)

                    if st.button("⬆️ Insert Uploaded CSV Data"):
                        self._insert_uploaded_data(df, selected_table)

                elif file.name.endswith(".xlsx"):
                    excel_file = pd.ExcelFile(file)
                    sheet_names = excel_file.sheet_names

                    selected_sheets = st.multiselect("Select sheet(s) to upload", sheet_names)

                    for sheet in selected_sheets:
                        st.markdown(f"### 📄 Sheet: {sheet}")
                        sheet_df = excel_file.parse(sheet)
                        sheet_df.columns = sheet_df.columns.str.strip().str.upper()
                        st.dataframe(sheet_df, use_container_width=True)

                        if st.button(f"⬆️ Insert Data from {sheet}", key=f"upload_{sheet}"):
                            self._insert_uploaded_data(sheet_df, selected_table)

            except Exception as e:
                st.error(f"❌ Error processing file: {e}")

    def _insert_uploaded_data(self, df, selected_table):
        # Fetch column metadata from Snowflake
        column_metadata_query = f"""
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
            FROM {st.session_state["selected_db"]}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{selected_table}' AND TABLE_SCHEMA = '{st.session_state["selected_schema"]}'
        """
        column_metadata = self.session.sql(column_metadata_query).collect()

        changes = 0
        for _, row in df.iterrows():
            # Validate the row using the _validate_row method
            #st.write(row)
            errors = self._validate_row(row, column_metadata, f"{selected_table}",f"{self.selected_schema}",f"{self.selected_db}")

            # If there are validation errors, show them and skip inserting the row
            if errors:
                for error in errors:
                    st.error(f"❌ {error}")
                continue

            # If no errors, insert the data
            cols = ", ".join(df.columns)
            vals = ", ".join([f"'{str(v)}'" if pd.notna(v) else "NULL" for v in row])
            insert_query = f"INSERT INTO {self.selected_db}.{self.selected_schema}.{selected_table} ({cols}) VALUES ({vals})"
            try:
                self.session.sql(insert_query).collect()
                changes += 1
            except Exception as e:
                st.error(f"❌ Error inserting row: {e}")

        if changes > 0:
            st.success(f"✅ {changes} rows saved successfully.")
        else:
            st.info("No new data inserted.")

    def app_tabs(self):
        st.markdown("## 📊 PRISM Master Data Application ")
        st.markdown("Enter or Upload your Master data ")
        tabs = st.tabs(["📁 View Data", "📤 Upload File"])

        # --- View Data Tab ---
        with tabs[0]:
            self._view_data_with_pagination()

            # st.markdown("---")
            # st.subheader("➕ Add Rows")
            # add_data()

        with tabs[1]:
            self._upload_file()

def main():
    app = SnowflakeDataApp()
    app._sidebar()
    app.app_tabs()

if __name__ == "__main__":
    main()
