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
          "ODBC basic" = "odbc_basic",
          "DSN (if configured)" = "dsn"
        ),
        selected = "odbc_basic"
      ),
      textInput("warehouse", "Warehouse (optional):", value = ""),
      actionButton("test_connection", "Test Connection", class = "btn-secondary")
    ),
    card(
      card_header("Query Input"),
      textAreaInput(
        "sql_query",
        "SQL Query:",
        value = "SHOW TABLES IN DEMO_DATA.PUBLIC;",
        rows = 4,
        placeholder = "Enter your SQL query here..."
      ),
      actionButton("execute_query", "Execute Query", class = "btn-primary"),
      hr(),
      h6("Exploration Queries:"),
      actionButton("query_show_tables", "Show tables in PUBLIC", class = "btn-outline-secondary btn-sm", style = "margin: 2px;"),
      actionButton("query_show_schemas", "Show schemas", class = "btn-outline-secondary btn-sm", style = "margin: 2px;"),
      actionButton("query_describe_tables", "Describe all tables", class = "btn-outline-secondary btn-sm", style = "margin: 2px;"),
      hr(),
      h6("MTCARS Queries:"),
      actionButton("query_mtcars_all", "All mtcars data", class = "btn-outline-secondary btn-sm", style = "margin: 2px;"),
      actionButton("query_mtcars_summary", "mtcars summary", class = "btn-outline-secondary btn-sm", style = "margin: 2px;"),
      actionButton("query_mtcars_describe", "Describe mtcars", class = "btn-outline-secondary btn-sm", style = "margin: 2px;")
    )
  ),
  card(
    card_header("Query Results"),
    DTOutput("query_results")
  ),
  card(
    card_header("Connection Information & Last Error"),
    verbatimTextOutput("connection_info")
  )
)

# Server
server <- function(input, output, session) {
  # Store last error for debugging
  last_error <- reactiveVal("")
  
  # Reactive connection object
  snowflake_conn <- reactive({
    # Get connection parameters from environment variables
    username <- Sys.getenv("SNOWFLAKE_USER")
    password <- Sys.getenv("SNOWFLAKE_PASSWORD")
    server <- Sys.getenv("SNOWFLAKE_SERVER")
    
    # Check if environment variables are set
    if (username == "" || password == "" || server == "") {
      return(list(status = "error", message = "Missing environment variables"))
    }
    
    tryCatch({
      conn <- switch(input$connection_method,
        "odbc_full" = {
          # Full connection with DEMO_DATA database and PUBLIC schema
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
          # Basic connection without database/schema
          dbConnect(
            odbc::odbc(),
            driver = "Snowflake",
            server = server,
            uid = username,
            pwd = password,
            warehouse = if(input$warehouse != "") input$warehouse else NULL
          )
        },
        "dsn" = {
          # DSN connection (requires pre-configured DSN)
          dbConnect(
            odbc::odbc(),
            dsn = "snowflake_dsn",
            uid = username,
            pwd = password
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
        "odbc_basic" = "✅ Connected to Snowflake\n(basic connection)",
        "dsn" = "✅ Connected via DSN"
      )
      method_desc
    }
  })
  
  # Connection information output
  output$connection_info <- renderText({
    username <- Sys.getenv("SNOWFLAKE_USER")
    server <- Sys.getenv("SNOWFLAKE_SERVER")
    
    info_text <- paste(
      "Environment Variables:",
      paste("SNOWFLAKE_USER:", ifelse(username != "", username, "Not set")),
      paste("SNOWFLAKE_SERVER:", ifelse(server != "", server, "Not set")),
      paste("SNOWFLAKE_PASSWORD:", ifelse(Sys.getenv("SNOWFLAKE_PASSWORD") != "", "Set", "Not set")),
      "",
      "Connection Settings:",
      paste("Method:", input$connection_method),
      paste("Database: DEMO_DATA"),
      paste("Schema: PUBLIC"),
      paste("Warehouse:", ifelse(input$warehouse != "", input$warehouse, "Not specified")),
      sep = "\n"
    )
    
    # Add last error if available
    if (last_error() != "") {
      info_text <- paste(
        info_text,
        "",
        "Last Query Error:",
        last_error(),
        sep = "\n"
      )
    }
    
    info_text
  })
  
  # Quick query button handlers
  observeEvent(input$query_show_tables, {
    updateTextAreaInput(session, "sql_query", value = "SHOW TABLES IN DEMO_DATA.PUBLIC;")
  })
  
  observeEvent(input$query_show_schemas, {
    updateTextAreaInput(session, "sql_query", value = "SHOW SCHEMAS IN DATABASE DEMO_DATA;")
  })
  
  observeEvent(input$query_describe_tables, {
    updateTextAreaInput(session, "sql_query", 
                       value = "SELECT \n  table_name, \n  table_type, \n  row_count, \n  bytes\nFROM DEMO_DATA.INFORMATION_SCHEMA.TABLES \nWHERE table_schema = 'PUBLIC'\nORDER BY table_name;")
  })
  
  observeEvent(input$query_mtcars_describe, {
    updateTextAreaInput(session, "sql_query", value = "DESCRIBE TABLE DEMO_DATA.PUBLIC.MTCARS;")
  })
  
  observeEvent(input$query_mtcars_all, {
    updateTextAreaInput(session, "sql_query", value = "SELECT * FROM DEMO_DATA.PUBLIC.MTCARS ORDER BY MPG DESC;")
  })
  
  observeEvent(input$query_mtcars_summary, {
    updateTextAreaInput(session, "sql_query", 
                       value = "SELECT \n  COUNT(*) as total_cars,\n  ROUND(AVG(MPG), 2) as avg_mpg,\n  ROUND(AVG(HP), 2) as avg_horsepower,\n  COUNT(DISTINCT CYL) as cylinder_types\nFROM DEMO_DATA.PUBLIC.MTCARS;")
  })
  
  # Query results
  query_data <- eventReactive(input$execute_query, {
    conn_result <- snowflake_conn()
    
    if (conn_result$status == "error") {
      last_error("")  # Clear last error
      return(data.frame(Error = conn_result$message))
    }
    
    if (input$sql_query == "") {
      last_error("")  # Clear last error
      return(data.frame(Error = "Please enter a SQL query"))
    }
    
    tryCatch({
      result <- dbGetQuery(conn_result$connection, input$sql_query)
      last_error("")  # Clear last error on success
      return(result)
    }, error = function(e) {
      # Store detailed error message
      error_msg <- paste("Query failed:", e$message)
      last_error(error_msg)
      return(data.frame(Error = error_msg))
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

# Run the app
shinyApp(ui = ui, server = server)
