dataset$OrderDate <- as.Date(dataset$OrderDate)

dataset <- dataset[!is.na(dataset$OrderDate) & !is.na(dataset$SalesAmount), ]

sales_by_date <- aggregate(
  SalesAmount ~ OrderDate,
  data = dataset,
  sum
)

plot(
  sales_by_date$OrderDate,
  sales_by_date$SalesAmount,
  type = "l",
  col = "blue",
  lwd = 2,
  xlab = "Date",
  ylab = "Total Sales",
  main = "Sales Trend using R Script Visual"
)
