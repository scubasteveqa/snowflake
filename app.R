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
        value = "SELECT * FROM DEMO_DATA.PUBLIC.MTCARS LIMIT 10;",
        rows = 4,
        placeholder = "Enter your SQL query here..."
      ),
      actionButton("execute_query", "Execute Query", class = "btn-primary"),
      hr(),
      h6("Quick Queries:"),
      actionButton("query_mtcars_all", "All mtcars data", class = "btn-outline-secondary btn-sm", style = "margin: 2px;"),
      actionButton("query_mtcars_summary", "mtcars summary", class = "btn-outline-secondary btn-sm", style = "margin: 2px;"),
      actionButton("query_show_tables", "Show tables", class = "btn-outline-secondary btn-sm", style = "margin: 2px;"),
      actionButton("query_show_schemas", "Show schemas", class = "btn-outline-secondary btn-sm", style = "margin: 2px;")
    )
  ),
  card(
    card_header("Query Results"),
    DTOutput("query_results")
  ),
  card(
    card_header("Connection Information"),
    verbatimTextOutput("connection_info")
  )
)

# Server
server <- function(input, output, session) {
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
    
    paste(
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
  })
  
  # Quick query button handlers
  observeEvent(input$query_mtcars_all, {
    query_text <- if(input$connection_method == "odbc_full") {
      "SELECT * FROM MTCARS ORDER BY MPG DESC;"
    } else {
      "SELECT * FROM DEMO_DATA.PUBLIC.MTCARS ORDER BY MPG DESC;"
    }
    updateTextAreaInput(session, "sql_query", value = query_text)
  })
  
  observeEvent(input$query_mtcars_summary, {
    query_text <- if(input$connection_method == "odbc_full") {
      "SELECT \n  COUNT(*) as total_cars,\n  ROUND(AVG(MPG), 2) as avg_mpg,\n  ROUND(AVG(HP), 2) as avg_horsepower,\n  COUNT(DISTINCT CYL) as cylinder_types,\n  MIN(YEAR) as oldest_year,\n  MAX(YEAR) as newest_year\nFROM MTCARS;"
    } else {
      "SELECT \n  COUNT(*) as total_cars,\n  ROUND(AVG(MPG), 2) as avg_mpg,\n  ROUND(AVG(HP), 2) as avg_horsepower,\n  COUNT(DISTINCT CYL) as cylinder_types,\n  MIN(YEAR) as oldest_year,\n  MAX(YEAR) as newest_year\nFROM DEMO_DATA.PUBLIC.MTCARS;"
    }
    updateTextAreaInput(session, "sql_query", value = query_text)
  })
  
  observeEvent(input$query_show_tables, {
    query_text <- if(input$connection_method == "odbc_full") {
      "SHOW TABLES IN SCHEMA PUBLIC;"
    } else {
      "SHOW TABLES IN DEMO_DATA.PUBLIC;"
    }
    updateTextAreaInput(session, "sql_query", value = query_text)
  })
  
  observeEvent(input$query_show_schemas, {
    updateTextAreaInput(session, "sql_query", value = "SHOW SCHEMAS IN DATABASE DEMO_DATA;")
  })
  
  # Query results
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

# Run the app
shinyApp(ui = ui, server = server)
