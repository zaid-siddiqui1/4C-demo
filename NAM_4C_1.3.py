import streamlit as st
import pandas as pd
import snowflake.connector

# Get Snowflake connection
def get_snowflake_connection():
    if 'snowflake_conn' not in st.session_state:
        conn = snowflake.connector.connect(
            user=st.secrets["snowflake"]["user"],
            account=st.secrets["snowflake"]["account"],
            warehouse=st.secrets["snowflake"]["warehouse"],
            authenticator='externalbrowser',
            client_session_keep_alive=True,
            database="PROD_TEAMS_DB",
            schema="COMMERCIAL_AUTOMATION_NAM"
        )
        st.session_state.snowflake_conn = conn
    return st.session_state.snowflake_conn

# Get fresh Snowflake connection
def get_fresh_snowflake_connection():
    return snowflake.connector.connect(
        user=st.secrets["snowflake"]["user"],
        account=st.secrets["snowflake"]["account"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        authenticator='externalbrowser',
        client_session_keep_alive=True,
        database="PROD_TEAMS_DB",
        schema="COMMERCIAL_AUTOMATION_NAM"
    )

# Get data from Snowflake
def get_data_from_snowflake():
    connection = st.session_state.snowflake_conn
    query = "SELECT * FROM PROD_TEAMS_DB.COMMERCIAL_AUTOMATION_NAM.VISTA_4C"
    df = pd.read_sql(query, connection)
    return df

# Clear the form state
def clear_new_record_form():
    st.session_state.new_concern = ''
    st.session_state.new_cause = ''
    st.session_state.new_countermeasure = ''
    st.session_state.new_check = ''
    st.session_state.new_owner = ''
    st.session_state.new_due_date = None
    st.session_state.new_status = ''

# Function to get next ID
def get_next_id():
    connection = st.session_state.snowflake_conn
    query = "SELECT MAX(ID) FROM PROD_TEAMS_DB.COMMERCIAL_AUTOMATION_NAM.VISTA_4C"
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchone()
    max_id = result[0] if result[0] is not None else 0
    return max_id + 1

def create_new_record():
    st.subheader("Create a New 4C Record")

    # Input fields for the new record
    concern = st.text_area('Concern', key='new_concern')
    cause = st.text_area('Cause', key='new_cause')
    countermeasure = st.text_area('Countermeasure', key='new_countermeasure')
    check = st.text_area('Check', key='new_check')

    # Other attributes
    owner = st.text_input("Owner email", key='new_owner')
    due_date = st.date_input("Due date", key='new_due_date')
    status_options = ['Not Started', 'On Track', 'Delayed', 'Completed', 'Cancelled']
    status = st.selectbox("Status", status_options, key='new_status')

    # Save the new record
    if st.button("Save New 4C"):
        # Validate required fields
        if not concern or not owner or not status:
            st.error("Please fill in all required fields: Concern, Owner, and Status.")
            return

        # Get the next ID
        new_id = get_next_id()

        # Create a fresh Snowflake connection
        connection = get_fresh_snowflake_connection()
        cursor = connection.cursor()

        # INSERT query with 'CREATED_DATE' and 'UPDATE_DATE'
        insert_query = """
        INSERT INTO PROD_TEAMS_DB.COMMERCIAL_AUTOMATION_NAM.VISTA_4C (
            ID, CONCERN_4C, CAUSE_4C, COUNTERMEASURE_4C, CHECK_4C,
            OWNER, STATUS, DUE_DATE, CREATED_DATE, UPDATE_DATE
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_DATE, CURRENT_DATE)
        """

        try:
            cursor.execute(insert_query, (
                new_id,
                concern,
                cause,
                countermeasure,
                check,
                owner,
                status,
                due_date
            ))
            connection.commit()
            st.success("New 4C record created successfully!")
            clear_new_record_form()
            connection.close()
            st.session_state.create_new = False
            st.rerun()
        except Exception as e:
            st.error(f"An error occurred: {e}")
            connection.close()

def main():
    st.title("Commercial Excellence 4C Tracker")
    st.write("Consumer NAM/N.LATAM | Leader: Berthold Zeep")

    # Initialize Snowflake connection
    if 'snowflake_conn' not in st.session_state:
        get_snowflake_connection()

    # Initialize the create_new flag in session_state
    if 'create_new' not in st.session_state:
        st.session_state.create_new = False

    # Handle "Create New 4C" button
    if st.button("Create New 4C"):
        st.session_state.create_new = True

    # If creating a new record, display the form and exit the function
    if st.session_state.create_new:
        create_new_record()
        return  # Exit to prevent displaying the rest of the page

    # Get the dataframe
    df = get_data_from_snowflake()

    if df.empty:
        st.success('No 4C records available.')
    else:
        # Display the dataframe/table showing all existing 4Cs
        st.subheader("4C Records")
        st.dataframe(df, use_container_width=True)

        st.write("---")

        # Now, select a record to edit
        st.subheader("Edit an Existing 4C Record")

        # Create a list of options for the selectbox
        df['Selection'] = df.apply(lambda row: f"ID: {row['ID']} - {row['CONCERN_4C'][:50]}", axis=1)
        selection_options = df['Selection'].tolist()

        # Add a selectbox to select a record
        selected_option = st.selectbox('Select a 4C record to edit', options=selection_options)

        # Find the selected row
        selected_index = selection_options.index(selected_option)
        selected_row = df.iloc[selected_index]

        st.subheader(f"Selected 4C ID: {selected_row['ID']}")

        # Create a fresh Snowflake connection for the update
        connection = get_fresh_snowflake_connection()

        # Layout for editing details
        col1_4C, col3_4C = st.columns([0.49, 0.49])

        with col1_4C:
            concern = st.text_area('Concern', selected_row['CONCERN_4C'], key='edit_concern')
            cause = st.text_area('Cause', selected_row['CAUSE_4C'], key='edit_cause')

        with col3_4C:
            countermeasure = st.text_area('Countermeasure', selected_row['COUNTERMEASURE_4C'], key='edit_countermeasure')
            check = st.text_area('Check', selected_row['CHECK_4C'], key='edit_check')

        st.write("---")

        # Layout for other attributes
        col1, col2, col3 = st.columns([0.33, 0.33, 0.33])

        with col1:
            owner = st.text_input("Owner email", selected_row['OWNER'], key='edit_owner')

        with col2:
            # Convert DUE_DATE to datetime.date object if it's not None
            if pd.notnull(selected_row['DUE_DATE']):
                due_date = pd.to_datetime(selected_row['DUE_DATE']).date()
            else:
                due_date = None
            due_date = st.date_input("Due date", due_date, key='edit_due_date')

        with col3:
            status_options = ['Not Started', 'On Track', 'Delayed', 'Completed', 'Cancelled']
            status = st.selectbox(
                "Status",
                status_options,
                index=status_options.index(selected_row['STATUS']) if selected_row['STATUS'] in status_options else 0,
                key='edit_status'
            )

        st.write("---")

        # Save changes to Snowflake
        if st.button("Save 4C"):
            # Prepare and execute the update query with parameters
            cursor = connection.cursor()
            update_query = """
            UPDATE PROD_TEAMS_DB.COMMERCIAL_AUTOMATION_NAM.VISTA_4C SET
            CONCERN_4C=%s,
            CAUSE_4C=%s,
            COUNTERMEASURE_4C=%s,
            CHECK_4C=%s,
            OWNER=%s,
            STATUS=%s,
            DUE_DATE=%s,
            UPDATE_DATE=CURRENT_DATE
            WHERE ID = %s
            """
            try:
                cursor.execute(update_query, (
                    st.session_state.edit_concern,
                    st.session_state.edit_cause,
                    st.session_state.edit_countermeasure,
                    st.session_state.edit_check,
                    st.session_state.edit_owner,
                    status,
                    due_date,
                    selected_row['ID']
                ))
                connection.commit()
                st.success("Data updated successfully!")
                connection.close()
                st.rerun()
            except Exception as e:
                st.error(f"An error occurred: {e}")
                connection.close()

if __name__ == '__main__':
    main()
