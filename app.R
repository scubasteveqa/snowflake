library(shiny)
library(bslib)
library(DBI)
library(odbc)
library(DT)

# Simple diagnostic Shiny app
ui <- page_sidebar(
  title = "Snowflake Connection Diagnostic",
  sidebar = sidebar(
    width = 300,
    card(
      card_header("Connection Test"),
      actionButton("test_conn", "Test Connection", class = "btn-primary"),
      verbatimTextOutput("conn_status")
    ),
    card(
      card_header("Table Discovery"),
      actionButton("list_databases", "List Databases", class = "btn-secondary"),
      actionButton("list_schemas", "List Schemas", class = "btn-secondary"),
      actionButton("list_tables", "List Tables", class = "btn-secondary")
    ),
    card(
      card_header("MTCARS Tests"),
      actionButton("test_mtcars_exists", "Check MTCARS Exists", class = "btn-info"),
      actionButton("test_mtcars_count", "Count MTCARS Rows", class = "btn-info"),
      actionButton("test_mtcars_simple", "Simple MTCARS Query", class = "btn-info")
    )
  ),
  card(
    card_header("Results"),
    DTOutput("results_table"),
    verbatimTextOutput("debug_output")
  )
)

server <- function(input, output, session) {
  
  # Establish connection
  get_connection <- function() {
    username <- Sys.getenv("SNOWFLAKE_USER")
    password <- Sys.getenv("SNOWFLAKE_PASSWORD")
    server <- Sys.getenv("SNOWFLAKE_SERVER")
    
    if (username == "" || password == "" || server == "") {
      stop("Missing environment variables")
    }
    
    dbConnect(
      odbc::odbc(),
      driver = "Snowflake",
      server = server,
      uid = username,
      pwd = password,
      database = "DEMO_DATA",
      schema = "PUBLIC"
    )
  }
  
  # Test connection
  observeEvent(input$test_conn, {
    output$conn_status <- renderText({
      tryCatch({
        conn <- get_connection()
        result <- dbGetQuery(conn, "SELECT CURRENT_USER() as user, CURRENT_DATABASE() as db, CURRENT_SCHEMA() as schema")
        dbDisconnect(conn)
        paste("✅ Connection successful!\nUser:", result$USER, 
              "\nDatabase:", result$DB, 
              "\nSchema:", result$SCHEMA)
      }, error = function(e) {
        paste("❌ Connection failed:", e$message)
      })
    })
  })
  
  # List databases
  observeEvent(input$list_databases, {
    output$results_table <- renderDT({
      tryCatch({
        conn <- get_connection()
        result <- dbGetQuery(conn, "SHOW DATABASES")
        dbDisconnect(conn)
        result
      }, error = function(e) {
        data.frame(Error = e$message)
      })
    })
  })
  
  # List schemas
  observeEvent(input$list_schemas, {
    output$results_table <- renderDT({
      tryCatch({
        conn <- get_connection()
        result <- dbGetQuery(conn, "SHOW SCHEMAS IN DEMO_DATA")
        dbDisconnect(conn)
        result
      }, error = function(e) {
        data.frame(Error = e$message)
      })
    })
  })
  
  # List tables
  observeEvent(input$list_tables, {
    output$results_table <- renderDT({
      tryCatch({
        conn <- get_connection()
        result <- dbGetQuery(conn, "SHOW TABLES IN DEMO_DATA.PUBLIC")
        dbDisconnect(conn)
        result
      }, error = function(e) {
        data.frame(Error = e$message)
      })
    })
  })
  
  # Check if mtcars exists
  observeEvent(input$test_mtcars_exists, {
    output$debug_output <- renderText({
      tryCatch({
        conn <- get_connection()
        
        # Try different case variations
        queries <- c(
          "SELECT table_name FROM DEMO_DATA.INFORMATION_SCHEMA.TABLES WHERE table_name LIKE '%mtcars%'",
          "SELECT table_name FROM DEMO_DATA.INFORMATION_SCHEMA.TABLES WHERE UPPER(table_name) LIKE '%MTCARS%'",
          "SELECT column_name, data_type FROM DEMO_DATA.INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'mtcars'",
          "SELECT column_name, data_type FROM DEMO_DATA.INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'MTCARS'"
        )
        
        results <- list()
        for (i in seq_along(queries)) {
          results[[i]] <- tryCatch({
            dbGetQuery(conn, queries[i])
          }, error = function(e) {
            data.frame(error = e$message)
          })
        }
        
        dbDisconnect(conn)
        
        paste("Table search results:",
              "\n1. Lowercase search:", nrow(results[[1]]), "rows",
              "\n2. Uppercase search:", nrow(results[[2]]), "rows",
              "\n3. Columns (mtcars):", nrow(results[[3]]), "rows",
              "\n4. Columns (MTCARS):", nrow(results[[4]]), "rows")
        
      }, error = function(e) {
        paste("Error:", e$message)
      })
    })
  })
  
  # Count mtcars rows
  observeEvent(input$test_mtcars_count, {
    output$debug_output <- renderText({
      tryCatch({
        conn <- get_connection()
        
        # Try different table name formats
        table_formats <- c(
          'DEMO_DATA.PUBLIC."mtcars"',
          'DEMO_DATA.PUBLIC.mtcars',
          'DEMO_DATA.PUBLIC.MTCARS',
          '"DEMO_DATA"."PUBLIC"."mtcars"'
        )
        
        results <- character(length(table_formats))
        for (i in seq_along(table_formats)) {
          results[i] <- tryCatch({
            query <- paste("SELECT COUNT(*) as count FROM", table_formats[i])
            result <- dbGetQuery(conn, query)
            paste(table_formats[i], ":", result$COUNT, "rows")
          }, error = function(e) {
            paste(table_formats[i], ": ERROR -", e$message)
          })
        }
        
        dbDisconnect(conn)
        paste(results, collapse = "\n")
        
      }, error = function(e) {
        paste("Connection error:", e$message)
      })
    })
  })
  
  # Simple mtcars query
  observeEvent(input$test_mtcars_simple, {
    output$results_table <- renderDT({
      tryCatch({
        conn <- get_connection()
        
        # First, find the correct table name
        tables <- dbGetQuery(conn, "SHOW TABLES IN DEMO_DATA.PUBLIC")
        mtcars_table <- tables[grepl("mtcars", tables$name, ignore.case = TRUE), ]
        
        if (nrow(mtcars_table) == 0) {
          dbDisconnect(conn)
          return(data.frame(Error = "No mtcars table found"))
        }
        
        # Use the actual table name found
        table_name <- mtcars_table$name[1]
        query <- paste0('SELECT * FROM DEMO_DATA.PUBLIC."', table_name, '" LIMIT 5')
        
        result <- dbGetQuery(conn, query)
        dbDisconnect(conn)
        result
        
      }, error = function(e) {
        data.frame(Error = paste("Query failed:", e$message))
      })
    })
  })
}

shinyApp(ui = ui, server = server)
