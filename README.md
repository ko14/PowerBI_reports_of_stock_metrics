# PowerBI_reports_of_stock_metrics

This repo is more about sharing data and reports than code, though I show my python code for generating the csv file from pre-fetched stock data in the generate_csv_data folder.  If you want a method for getting stock data, I share one in [my other repo](https://github.com/ko14/website-data).

The csv file is automatically posted to this repo after each trading day.  

The html file can be downloaded to your device to view the pbix report that is published on the PowerBI online portal, which in turn gets its data refreshed nightly from the csv file in this repo.  The pbix report is included here as well.

The stocks in the csv are those with at least $1B market cap, 1M avg trading volume, and with prices for all 20 past trading days.  

Backend data collection and csv are genarated on AWS with lambdas, S3, Glue, Athena, Step Functions.

![image](https://github.com/ko14/PowerBI_reports_of_stock_metrics/assets/3990085/ec4f9471-e511-41d8-b694-7bdab3b4c98c)



