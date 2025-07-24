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
      card_header("Query Input"),
      textAreaInput(
        "sql_query",
        "SQL Query:",
        value = "SELECT * FROM demo_data.public.mtcars LIMIT 10;",
        rows = 4,
        placeholder = "Enter your SQL query here..."
      ),
      actionButton("execute_query", "Execute Query", class = "btn-primary"),
      hr(),
      h6("Quick Queries:"),
      actionButton("query_mtcars_all", "All mtcars data", class = "btn-outline-secondary btn-sm", style = "margin: 2px;"),
      actionButton("query_mtcars_summary", "mtcars summary", class = "btn-outline-secondary btn-sm", style = "margin: 2px;"),
      actionButton("query_version", "Snowflake version", class = "btn-outline-secondary btn-sm", style = "margin: 2px;")
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
      return(NULL)
    }
    
    tryCatch({
      # Establish connection to Snowflake with database and schema
      conn <- dbConnect(
        odbc::odbc(),
        driver = "Snowflake",
        server = server,
        uid = username,
        pwd = password,
        database = "demo_data",
        schema = "public"
      )
      return(conn)
    }, error = function(e) {
      return(paste("Connection failed:", e$message))
    })
  })
  
  # Connection status output
  output$connection_status <- renderText({
    conn <- snowflake_conn()
    
    if (is.null(conn)) {
      "❌ Missing environment variables.\nPlease set:\n- SNOWFLAKE_USER\n- SNOWFLAKE_PASSWORD\n- SNOWFLAKE_SERVER"
    } else if (is.character(conn)) {
      paste("❌", conn)
    } else {
      "✅ Connected to Snowflake\n📊 Database: demo_data\n📋 Schema: public"
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
      "DATABASE: demo_data",
      "SCHEMA: public",
      sep = "\n"
    )
  })
  
  # Quick query button handlers
  observeEvent(input$query_mtcars_all, {
    updateTextAreaInput(session, "sql_query", 
                        value = "SELECT * FROM demo_data.public.mtcars;")
  })
  
  observeEvent(input$query_mtcars_summary, {
    updateTextAreaInput(session, "sql_query", 
                        value = "SELECT \n  COUNT(*) as total_cars,\n  AVG(mpg) as avg_mpg,\n  AVG(hp) as avg_horsepower,\n  COUNT(DISTINCT cyl) as cylinder_types\nFROM demo_data.public.mtcars;")
  })
  
  observeEvent(input$query_version, {
    updateTextAreaInput(session, "sql_query", 
                        value = "SELECT CURRENT_VERSION() as snowflake_version;")
  })
  
  # Query results
  query_data <- eventReactive(input$execute_query, {
    conn <- snowflake_conn()
    
    if (is.null(conn) || is.character(conn)) {
      return(data.frame(Error = "No valid database connection"))
    }
    
    if (input$sql_query == "") {
      return(data.frame(Error = "Please enter a SQL query"))
    }
    
    tryCatch({
      result <- dbGetQuery(conn, input$sql_query)
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
    conn <- snowflake_conn()
    if (!is.null(conn) && !is.character(conn)) {
      dbDisconnect(conn)
    }
  })
}

# Run the app
shinyApp(ui = ui, server = server)