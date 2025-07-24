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
          "ODBC with database/schema" = "odbc_full",
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
        value = "SELECT CURRENT_VERSION() as version;",
        rows = 4,
        placeholder = "Enter your SQL query here..."
      ),
      actionButton("execute_query", "Execute Query", class = "btn-primary"),
      hr(),
      h6("Quick Queries:"),
      actionButton("query_mtcars_all", "All mtcars data", class = "btn-outline-secondary btn-sm", style = "margin: 2px;"),
      actionButton("query_mtcars_summary", "mtcars summary", class = "btn-outline-secondary btn-sm", style = "margin: 2px;"),
      actionButton("query_show_databases", "Show databases", class = "btn-outline-secondary btn-sm", style = "margin: 2px;")
    )
  ),
  card(
    card_header("Query Results"),
    DTOutput("query_results")
  ),
  card(
    card_header("Connection Information & Debug"),
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
          # Full connection with database and schema
          dbConnect(
            odbc::odbc(),
            driver = "Snowflake",
            server = server,
            uid = username,
            pwd = password,
            database = "demo_data",
            schema = "public",
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
        "odbc_full" = "with demo_data.public",
        "odbc_basic" = "basic (no default DB)",
        "dsn" = "via DSN"
      )
      paste("✅ Connected to Snowflake", method_desc, sep = "\n")
    }
  })
  
  # Connection information output
  output$connection_info <- renderText({
    username <- Sys.getenv("SNOWFLAKE_USER")
    server <- Sys.getenv("SNOWFLAKE_SERVER")
    
    # Check available drivers
    drivers <- sort(odbc::odbcListDrivers()$name)
    snowflake_drivers <- drivers[grepl("snowflake|Snowflake", drivers, ignore.case = TRUE)]
    
    paste(
      "Environment Variables:",
      paste("SNOWFLAKE_USER:", ifelse(username != "", username, "Not set")),
      paste("SNOWFLAKE_SERVER:", ifelse(server != "", server, "Not set")),
      paste("SNOWFLAKE_PASSWORD:", ifelse(Sys.getenv("SNOWFLAKE_PASSWORD") != "", "Set", "Not set")),
      "",
      "Connection Method:", input$connection_method,
      paste("Warehouse:", ifelse(input$warehouse != "", input$warehouse, "Not specified")),
      "",
      "Available Snowflake Drivers:",
      if(length(snowflake_drivers) > 0) paste(snowflake_drivers, collapse = ", ") else "None found",
      "",
      "All ODBC Drivers:",
      paste(head(drivers, 10), collapse = ", "),
      if(length(drivers) > 10) "... (and more)" else "",
      sep = "\n"
    )
  })
  
  # Quick query button handlers
  observeEvent(input$query_mtcars_all, {
    query_text <- if(input$connection_method == "odbc_full") {
      "SELECT * FROM mtcars LIMIT 20;"
    } else {
      "SELECT * FROM demo_data.public.mtcars LIMIT 20;"
    }
    updateTextAreaInput(session, "sql_query", value = query_text)
  })
  
  observeEvent(input$query_mtcars_summary, {
    query_text <- if(input$connection_method == "odbc_full") {
      "SELECT \n  COUNT(*) as total_cars,\n  AVG(mpg) as avg_mpg,\n  AVG(hp) as avg_horsepower,\n  COUNT(DISTINCT cyl) as cylinder_types\nFROM mtcars;"
    } else {
      "SELECT \n  COUNT(*) as total_cars,\n  AVG(mpg) as avg_mpg,\n  AVG(hp) as avg_horsepower,\n  COUNT(DISTINCT cyl) as cylinder_types\nFROM demo_data.public.mtcars;"
    }
    updateTextAreaInput(session, "sql_query", value = query_text)
  })
  
  observeEvent(input$query_show_databases, {
    updateTextAreaInput(session, "sql_query", value = "SHOW DATABASES;")
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
