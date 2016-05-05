# SalesPredictions
<b>Forecasts Walmart sales based on weather data.</b>

Using data from a Kaggle competition, I predicted sales of given items at Walmart locations based on weather. Abstract is below; for more details, see ProjectReport.pdf. Project files here include python scripts but not dataset.

<b>Abstract—</b>Retailers must forecast customer demand in order to
stock the right products in the right quantities. Companies like
Walmart analyze sales data to prevent “a retailer's twin
nightmares: too much inventory, or not enough” [1]. For a
previous Kaggle competition, Walmart released a dataset for
45 stores over 15 months [2]. The data included the daily units
sold at each store of 111 items for which demand might vary
with the weather, such as milk or umbrellas. Data from the
weather station nearest each store was provided as well. Using
a collaborative filtering approach, I sought to predict sales of
each item at each store based on the daily average
temperature. I found the most accurate results by assigning
each temperature to a range, then using the range as the basis
of the predictions.
