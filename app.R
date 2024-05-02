library(shiny)
library(pool)
library(httr)
library(jsonlite)

# Define these to appropriate values
owner <- "jennifersp"
dbName <- "shiny-iris"
api <- "https://www.dolthub.com/api/v1alpha1"
tableName <- "iris_test"
columns <- c("sepal length", "sepal width", "petal length", "petal width", "class")

# Add your token here
authToken <- ""

# Add your dolthub user name here
username <- ""

# Database connection setup
db <- dbPool(
  drv = RMySQL::MySQL(),
  dbname = dbName,
  host = "127.0.0.1",
  port = 3306,
  username = username,
  password = ""
)

loadData <- function() {
  data <- dbGetQuery(db, sprintf("SELECT * FROM %s", tableName))
  data
}

getCurrentBranch <- function() {
  cb <- dbGetQuery(db, sprintf("SELECT active_branch()"))[[1]]
  cb
}

checkoutNewBranch <- function(newBranchName) {
  query <- sprintf("CALL DOLT_CHECKOUT('-b', '%s')", paste(newBranchName))
  dbGetQuery(db, query)
}

getBranchNames <- function() {
  query <- "SELECT name FROM dolt_branches"
  res <- dbGetQuery(db, query)
  return(res)
}

checkoutBranch <- function(branch) {
  query <- sprintf("CALL DOLT_CHECKOUT('%s')", paste(branch))
  dbGetQuery(db, query)
}

getDoltDiffOnWorkingSet <- function() {
  query <- sprintf("SELECT * FROM DOLT_DIFF('HEAD','WORKING','%s')", paste(tableName))
  diff <- dbGetQuery(db, query)
  diff
}

getIrisClassNames <- function() {
  q <- sprintf("SELECT class from `%s` GROUP BY class", tableName)
  res <- dbGetQuery(db, q)
  return(res)
}

insertData <- function(data) {
  # Construct the update query by looping over the columns
  query <- sprintf(
    "INSERT INTO %s (`%s`) VALUES ('%s')",
    tableName,
    paste(names(data), collapse = "`, `"),
    paste(data, collapse = "', '")
  )
  dbGetQuery(db, query)
}

commitData <- function(commitMessage) {
  if (is.null(commitMessage) || commitMessage == "") {
    commitMessage <- "commit my working set"
  }
  query <- sprintf("CALL DOLT_COMMIT('-Am', '%s')", paste(commitMessage))
  dbGetQuery(db, query)
}

push <- function(branch) {
  query <- sprintf("CALL DOLT_PUSH('--set-upstream', 'origin', '%s')", paste(branch))
  dbGetQuery(db, query)
}

pull <- function() {
  query <- "CALL DOLT_PULL()"
  dbGetQuery(db, query)
}

createPR <- function(t, d, fb, tb) {
  # create json object for request body
  pr <- data.frame(
    title = paste(t),
    description = paste(d),
    fromBranchOwnerName = owner,
    fromBranchRepoName = dbName,
    fromBranchName = paste(fb),
    toBranchOwnerName = owner,
    toBranchRepoName = dbName,
    toBranchName = paste(tb)
  )
  prBody <- toJSON(unbox(fromJSON(toJSON(pr))))
  url <- sprintf("%s/%s/%s/pulls", api, owner, dbName)
  # create POST request
  res <- POST(url,
              body = prBody,
              content_type("application/json"),
              add_headers("authorization" = sprintf("token %s", authToken))
  )
  response <- fromJSON(rawToChar(res$content))
  response$status
}

listPRs <- function() {
  l <- data.frame(
    owner = owner,
    database = dbName
  )
  listPRsBody <- toJSON(unbox(fromJSON(toJSON(l))))
  url <- sprintf("%s/%s/%s/pulls", api, owner, dbName)
  # create GET request
  res <- GET(url,
             body = listPRsBody,
             add_headers("authorization" = sprintf("token %s", authToken))
  )
  response <- fromJSON(rawToChar(res$content))
  return(setNames(response$pulls$pull_id, paste(response$pulls$title, ":", response$pulls$state)))
}

mergePR <- function(prId) {
  url <- sprintf("%s/%s/%s/pulls/%s/merge", api, owner, dbName, prId)
  # create POST request
  res <- POST(url,
              content_type("application/json"),
              add_headers("authorization" = sprintf("token %s", authToken))
  )
  response <- fromJSON(rawToChar(res$content))
  response$status
}

ui <- fluidPage(
  uiOutput("branchOptions"),
  textInput("newBranch", "", ""),
  actionButton("checkoutNew", "Checkout a New Branch"),
  DT::dataTableOutput("results", width = 700),
  div( style = "display: flex; justify-content: space-around;",
       numericInput("sepal length", "Sepal Length", "0.0", width="auto"),
       numericInput("sepal width", "Sepal Width", "0.0", width="auto"),
       numericInput("petal length", "Pedal Length", "0.0", width="auto"),
       numericInput("petal width", "Pedal Width", "0.0", width="auto"),
       selectInput("class", "Class", getIrisClassNames()),
       actionButton("insert", "Insert"),
  ),
  tableOutput("diffResults"),
  textInput("commitMsg", "Commit Message", ""),
  actionButton("commit", "Commit"),
  actionButton("push", "Push to Remote"),
  actionButton("pull", "Pull from Remote"),
  titlePanel("Create PR"),
  textInput("prTitle", "Title", ""),
  textInput("prDescription", "Description", ""),
  textInput("prFromBranch", "From", ""),
  textInput("prToBranch", "To", ""),
  actionButton("createpr", "Create a Pull Request"),
  uiOutput("PROptions"),
  actionButton("mergepr", "Merge Pull Request by ID"),
)

server <- function(input, output, session) {
  # Whenever a field is filled, aggregate all data
  formData <- reactive({
    data <- sapply(columns, function(x) input[[x]])
    data
  })

  # Checkout a new branch
  observeEvent(input$checkoutNew, {
    checkoutNewBranch(input$newBranch)
  })

  # Checkout an existing branch
  observeEvent(input$curBranch, {
    checkoutBranch(input$curBranch)
  })

  # Insert button
  observeEvent(input$insert, {
    insertData(formData())
  })

  # Commit button
  observeEvent(input$commit, {
    commitData(input$commitMsg)
  })

  # Push button
  observeEvent(input$push, {
    push(input$curBranch)
  })

  # Pull button
  observeEvent(input$pull, {
    pull()
  })

  # Create PR button
  observeEvent(input$createpr, {
    status <- createPR(input$prTitle, input$prDescription, input$prFromBranch, input$prToBranch)
    showNotification(status)
  })

  # When the Pull button is clicked, pull from remote
  observeEvent(input$mergepr, {
    status <- mergePR(input$prId)
    showNotification(status)
  })

  # updates the `iris_test` table data
  output$results <- DT::renderDataTable({
    # update the results whenever there is event triggered
    input$createNew
    input$curBranch
    input$insert
    input$pull
    loadData()
  })

  # updates the diff table data
  output$diffResults <- renderTable({
    # update the results whenever there is event triggered
    input$createNew
    input$curBranch
    input$insert
    input$commit
    getDoltDiffOnWorkingSet()
  })

  # updates the select inputs of existing branches
  output$branchOptions <- renderUI({
    input$curBranch
    input$checkoutNew
    selectInput("curBranch", "Branches:", getBranchNames()[[1]], selected = getCurrentBranch())
  })

  # updates the select inputs of existing pull requests, including open, closed and merged ones.
  output$PROptions <- renderUI({
    input$mergepr
    input$createpr
    selectInput("prId", "Pull Requests:", listPRs())
  })
}

# Clean up the database connection when the app exits
onStop(function() {
  poolClose(db)
})

# run Shiny app
shinyApp(ui, server)
