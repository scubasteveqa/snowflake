library(shiny)
library(bslib)
library(DBI)
library(odbc)
library(DT)

# UI
ui <- page_sidebar(
  title = "Snowflake Database Connection",
  sidebar = sidebar(
    width = 300,
    card(
      card_header("Connection Status"),
      verbatimTextOutput("connection_status")
    ),
    card(
      card_header("Connection Settings"),
      selectInput(
        "connection_method",
        "Connection Method:",
        choices = list(
          "ODBC with DEMO_DATA.PUBLIC" = "odbc_full",
          "ODBC basic" = "odbc_basic"
        ),
        selected = "odbc_full"
      ),
      textInput("warehouse", "Warehouse (optional):", value = ""),
      actionButton("test_connection", "Test Connection", class = "btn-secondary")
    ),
    card(
      card_header("Query Input"),
      textAreaInput(
        "sql_query",
        "SQL Query:",
        value = "SELECT * FROM DEMO_DATA.PUBLIC.\"mtcars\" LIMIT 10;",
        rows = 4,
        placeholder = "Enter your SQL query here..."
      ),
      actionButton("execute_query", "Execute Query", class = "btn-primary"),
      hr(),
      h6("Quick Queries:"),
      actionButton("query_simple", "Simple test", class = "btn-outline-success btn-sm", style = "margin: 2px;"),
      actionButton("query_show_tables", "Show tables", class = "btn-outline-secondary btn-sm", style = "margin: 2px;"),
      actionButton("query_table_info", "Table info", class = "btn-outline-secondary btn-sm", style = "margin: 2px;"),
      hr(),
      h6("MTCARS Queries:"),
      actionButton("query_mtcars_sample", "Sample (5 rows)", class = "btn-outline-primary btn-sm", style = "margin: 2px;"),
      actionButton("query_mtcars_all", "All data", class = "btn-outline-primary btn-sm", style = "margin: 2px;"),
      actionButton("query_mtcars_summary", "Summary stats", class = "btn-outline-primary btn-sm", style = "margin: 2px;"),
      actionButton("query_mtcars_describe", "Structure", class = "btn-outline-primary btn-sm", style = "margin: 2px;")
    )
  ),
  card(
    card_header("Query Results"),
    DTOutput("query_results")
  )
)

# Server
server <- function(input, output, session) {
  # Reactive connection object
  snowflake_conn <- reactive({
    username <- Sys.getenv("SNOWFLAKE_USER")
    password <- Sys.getenv("SNOWFLAKE_PASSWORD")
    server <- Sys.getenv("SNOWFLAKE_SERVER")
    
    if (username == "" || password == "" || server == "") {
      return(list(status = "error", message = "Missing environment variables"))
    }
    
    tryCatch({
      conn <- switch(input$connection_method,
        "odbc_full" = {
          dbConnect(
            odbc::odbc(),
            driver = "Snowflake",
            server = server,
            uid = username,
            pwd = password,
            database = "DEMO_DATA",
            schema = "PUBLIC",
            warehouse = if(input$warehouse != "") input$warehouse else NULL
          )
        },
        "odbc_basic" = {
          dbConnect(
            odbc::odbc(),
            driver = "Snowflake",
            server = server,
            uid = username,
            pwd = password,
            warehouse = if(input$warehouse != "") input$warehouse else NULL
          )
        }
      )
      
      return(list(status = "success", connection = conn))
      
    }, error = function(e) {
      return(list(status = "error", message = paste("Connection failed:", e$message)))
    })
  })
  
  # Test connection button
  observeEvent(input$test_connection, {
    showNotification("Testing connection...", type = "message", duration = 2)
    conn_result <- snowflake_conn()
    if (conn_result$status == "success") {
      showNotification("✅ Connection successful!", type = "success", duration = 3)
    } else {
      showNotification("❌ Connection failed", type = "error", duration = 5)
    }
  })
  
  # Connection status output
  output$connection_status <- renderText({
    conn_result <- snowflake_conn()
    
    if (conn_result$status == "error") {
      if (grepl("Missing environment variables", conn_result$message)) {
        "❌ Missing environment variables.\nPlease set:\n- SNOWFLAKE_USER\n- SNOWFLAKE_PASSWORD\n- SNOWFLAKE_SERVER"
      } else {
        paste("❌", conn_result$message)
      }
    } else {
      method_desc <- switch(input$connection_method,
        "odbc_full" = "✅ Connected to Snowflake\n📊 Database: DEMO_DATA\n📋 Schema: PUBLIC",
        "odbc_basic" = "✅ Connected to Snowflake\n(basic connection)"
      )
      method_desc
    }
  })
  
  # Query button handlers
  observeEvent(input$query_simple, {
    updateTextAreaInput(session, "sql_query", value = "SELECT 1 as test_value, CURRENT_USER() as user;")
  })
  
  observeEvent(input$query_show_tables, {
    updateTextAreaInput(session, "sql_query", value = "SHOW TABLES IN DEMO_DATA.PUBLIC;")
  })
  
  observeEvent(input$query_table_info, {
    updateTextAreaInput(session, "sql_query", 
                       value = "SELECT \n  table_name, \n  table_type, \n  row_count, \n  bytes\nFROM DEMO_DATA.INFORMATION_SCHEMA.TABLES \nWHERE table_schema = 'PUBLIC'\nORDER BY table_name;")
  })
  
  observeEvent(input$query_mtcars_sample, {
    updateTextAreaInput(session, "sql_query", value = "SELECT * FROM DEMO_DATA.PUBLIC.\"mtcars\" LIMIT 5;")
  })
  
  observeEvent(input$query_mtcars_all, {
    updateTextAreaInput(session, "sql_query", value = "SELECT * FROM DEMO_DATA.PUBLIC.\"mtcars\" ORDER BY \"mpg\" DESC;")
  })
  
  observeEvent(input$query_mtcars_describe, {
    updateTextAreaInput(session, "sql_query", value = "DESCRIBE TABLE DEMO_DATA.PUBLIC.\"mtcars\";")
  })
  
  observeEvent(input$query_mtcars_summary, {
    updateTextAreaInput(session, "sql_query", 
                       value = "SELECT \n  COUNT(*) as total_cars,\n  ROUND(AVG(\"mpg\"), 2) as avg_mpg,\n  ROUND(AVG(\"hp\"), 2) as avg_horsepower,\n  COUNT(DISTINCT \"cyl\") as cylinder_types\nFROM DEMO_DATA.PUBLIC.\"mtcars\";")
  })

  # Add this to your observeEvent handlers:
  observeEvent(input$query_debug_tables, {
  updateTextAreaInput(session, "sql_query", 
                     value = "SELECT table_name, table_schema FROM DEMO_DATA.INFORMATION_SCHEMA.TABLES WHERE table_name LIKE '%mtcars%';")
  })

  observeEvent(input$query_debug_columns, {
  updateTextAreaInput(session, "sql_query", 
                     value = "SELECT column_name, data_type FROM DEMO_DATA.INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'mtcars' OR table_name = 'MTCARS';")
  })
      
  # Query execution
  query_data <- eventReactive(input$execute_query, {
    conn_result <- snowflake_conn()
    
    if (conn_result$status == "error") {
      return(data.frame(Error = conn_result$message))
    }
    
    if (input$sql_query == "") {
      return(data.frame(Error = "Please enter a SQL query"))
    }
    
    tryCatch({
      result <- dbGetQuery(conn_result$connection, input$sql_query)
      
      if (nrow(result) == 0) {
        return(data.frame(Message = "Query executed successfully but returned no rows"))
      }
      
      return(result)
    }, error = function(e) {
      return(data.frame(Error = paste("Query failed:", e$message)))
    })
  })
  
  output$query_results <- renderDT({
    query_data()
  }, options = list(scrollX = TRUE, pageLength = 10))
  
  # Clean up connection when session ends
  session$onSessionEnded(function() {
    conn_result <- snowflake_conn()
    if (conn_result$status == "success") {
      tryCatch({
        dbDisconnect(conn_result$connection)
      }, error = function(e) {
        # Ignore disconnection errors
      })
    }
  })
}

shinyApp(ui = ui, server = server)
