library(shiny)
library(bslib)
library(DBI)
library(odbc)
library(DT)

ui <- page_sidebar(
  title = "Snowflake ODBC Debug",
  sidebar = sidebar(
    width = 300,
    card(
      card_header("Connection Options"),
      checkboxInput("use_timeout", "Use Query Timeout", value = TRUE),
      checkboxInput("use_autocommit", "Use Autocommit", value = TRUE),
      numericInput("timeout_seconds", "Timeout (seconds):", value = 30, min = 5, max = 300)
    ),
    card(
      card_header("Query Tests"),
      actionButton("test_simple_select", "Test Simple SELECT", class = "btn-primary"),
      actionButton("test_mtcars_meta", "Test Table Metadata", class = "btn-secondary"),
      actionButton("test_mtcars_query", "Test MTCARS Query", class = "btn-success"),
      actionButton("test_alternative_method", "Try Alternative Method", class = "btn-info")
    )
  ),
  card(
    card_header("Results"),
    DTOutput("results_table"),
    verbatimTextOutput("debug_info")
  )
)

server <- function(input, output, session) {

  # Enhanced connection function with additional parameters
  get_enhanced_connection <- function() {
    username <- Sys.getenv("SNOWFLAKE_USER")
    password <- Sys.getenv("SNOWFLAKE_PASSWORD")
    server <- Sys.getenv("SNOWFLAKE_SERVER")

    if (username == "" || password == "" || server == "") {
      stop("Missing environment variables")
    }

    # Build connection string with additional parameters
    conn_params <- list(
      driver = "Snowflake",
      server = server,
      uid = username,
      pwd = password,
      database = "DEMO_DATA",
      schema = "PUBLIC"
    )

    # Add optional parameters
    if (input$use_timeout) {
      conn_params$timeout <- input$timeout_seconds
    }
    
    if (input$use_autocommit) {
      conn_params$autocommit <- TRUE
    }

    # Additional ODBC parameters that might help
    conn_params$warehouse <- ""  # Empty warehouse
    conn_params$role <- ""       # Default role
    conn_params$authenticator <- "snowflake"  # Explicit authenticator

    do.call(dbConnect, c(list(odbc::odbc()), conn_params))
  }

  # Test simple SELECT
  observeEvent(input$test_simple_select, {
    output$results_table <- renderDT({
      tryCatch({
        conn <- get_enhanced_connection()
        result <- dbGetQuery(conn, "SELECT 1 as test_col, CURRENT_USER() as user_col")
        dbDisconnect(conn)
        result
      }, error = function(e) {
        data.frame(Error = paste("Simple query failed:", e$message))
      })
    })
  })

  # Test table metadata approach
  observeEvent(input$test_mtcars_meta, {
    output$debug_info <- renderText({
      tryCatch({
        conn <- get_enhanced_connection()

        # Try getting table info first
        meta_query <- 'SELECT * FROM DEMO_DATA.INFORMATION_SCHEMA.TABLES WHERE table_name = \'mtcars\''
        meta_result <- dbGetQuery(conn, meta_query)

        # Try different approaches to access the table
        approaches <- list(
          "Direct with quotes" = 'SELECT COUNT(*) FROM DEMO_DATA.PUBLIC."mtcars"',
          "Using table function" = 'SELECT COUNT(*) FROM TABLE(\'DEMO_DATA.PUBLIC."mtcars"\')',
          "With explicit database context" = 'USE DATABASE DEMO_DATA; USE SCHEMA PUBLIC; SELECT COUNT(*) FROM "mtcars"'
        )

        results <- character(length(approaches))
        for (i in seq_along(approaches)) {
          results[i] <- tryCatch({
            if (names(approaches)[i] == "With explicit database context") {
              # Execute USE statements separately
              dbExecute(conn, "USE DATABASE DEMO_DATA")
              dbExecute(conn, "USE SCHEMA PUBLIC")
              result <- dbGetQuery(conn, 'SELECT COUNT(*) FROM "mtcars"')
            } else {
              result <- dbGetQuery(conn, approaches[[i]])
            }
            paste(names(approaches)[i], ": SUCCESS -", result[1,1], "rows")
          }, error = function(e) {
            paste(names(approaches)[i], ": ERROR -", substr(e$message, 1, 100))
          })
        }

        dbDisconnect(conn)
        paste(c(paste("Metadata rows:", nrow(meta_result)), results), collapse = "\n")

      }, error = function(e) {
        paste("Connection error:", e$message)
      })
    })
  })

  # Test mtcars query with different methods
  observeEvent(input$test_mtcars_query, {
    output$results_table <- renderDT({
      tryCatch({
        conn <- get_enhanced_connection()

        # Set context explicitly
        dbExecute(conn, "USE DATABASE DEMO_DATA")
        dbExecute(conn, "USE SCHEMA PUBLIC")

        # Try the simplest possible query
        result <- dbGetQuery(conn, 'SELECT * FROM "mtcars" LIMIT 3')

        dbDisconnect(conn)
        result

      }, error = function(e) {
        # If that fails, try dbFetch approach
        tryCatch({
          conn <- get_enhanced_connection()
          dbExecute(conn, "USE DATABASE DEMO_DATA")
          dbExecute(conn, "USE SCHEMA PUBLIC")

          # Use dbSendQuery + dbFetch instead of dbGetQuery
          res <- dbSendQuery(conn, 'SELECT * FROM "mtcars" LIMIT 3')
          result <- dbFetch(res)
          dbClearResult(res)
          dbDisconnect(conn)
          result

        }, error = function(e2) {
          data.frame(Error = paste("Both methods failed:",
                                 "dbGetQuery:", e$message,
                                 "dbSendQuery:", e2$message))
        })
      })
    })
  })

  # Try alternative connection method
  observeEvent(input$test_alternative_method, {
    output$results_table <- renderDT({
      tryCatch({
        username <- Sys.getenv("SNOWFLAKE_USER")
        password <- Sys.getenv("SNOWFLAKE_PASSWORD")
        server <- Sys.getenv("SNOWFLAKE_SERVER")

        # Try with minimal connection parameters
        conn <- dbConnect(
          odbc::odbc(),
          driver = "Snowflake",
          server = server,
          uid = username,
          pwd = password
        )

        # Set session parameters after connection
        dbExecute(conn, "USE DATABASE DEMO_DATA")
        dbExecute(conn, "USE SCHEMA PUBLIC")

        # Use fully qualified name without quotes first
        result <- tryCatch({
          dbGetQuery(conn, "SELECT * FROM DEMO_DATA.PUBLIC.mtcars LIMIT 3")
        }, error = function(e1) {
          # If unquoted fails, try quoted
          dbGetQuery(conn, 'SELECT * FROM DEMO_DATA.PUBLIC."mtcars" LIMIT 3')
        })

        dbDisconnect(conn)
        result

      }, error = function(e) {
        data.frame(Error = paste("Alternative method failed:", e$message))
      })
    })
  })
}

shinyApp(ui = ui, server = server)
