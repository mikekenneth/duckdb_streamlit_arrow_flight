import streamlit as st
from streamlit_ace import st_ace
from xlib.flight import execute_query


st.set_page_config(page_icon=":duck:", page_title="Remote DuckDB ⛁  Client with Arrow Flight RPC")

st.title("Remote DuckDB ⛁  Client with Arrow Flight")

with st.form(key="query_form"):
    st.markdown("## 🇸 🇶 🇱 Query Space")
    sql_query = st_ace(
        # value="select '1' as testing;"
        placeholder="Write your Query here...",
        language="sql",
        theme="dracula",
        keybinding="vscode",
        font_size=14,
        tab_size=4,
        show_gutter=True,
        show_print_margin=False,
        wrap=False,
        auto_update=True,
        readonly=False,
        min_lines=20,
        key="code_content",
    )
    submit = st.form_submit_button(label="Run Query")

if submit:
    # Run the Query agains Fligh Server
    result_flight_info, result_table = execute_query(sql_query)

    st.subheader("Results", divider="rainbow", anchor="results")
    tab1, tab2, tab3, tab4 = st.tabs(["Result", "Query", "Schema", "Info"])

    with tab1:
        st.dataframe(result_table.to_pandas(), use_container_width=True)

    with tab2:
        st.markdown(f"```sql\n {sql_query} \n```")

    with tab3:
        st.markdown(f"```csharp\n=== Result Schema ===\n{result_flight_info.schema}\n=====================\n```")

    with tab4:
        st.dataframe(
            {
                "Total Rows:": result_flight_info.total_records,
                "Total Size (Bytes):": result_flight_info.total_bytes,
                "Dataset Paths:": [i.decode("utf-8") for i in result_flight_info.descriptor.path],
            },
            hide_index=True,
        )
