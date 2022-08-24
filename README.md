# NordPoolCrawler
Crawler to scrape data from [NordPool website](https://www.nordpoolgroup.com/en/Market-data1/#/nordic/table)



#### Example: Supply/Demand curves
```
from curve_crawler import CurveCrawler, Curve
curve_range = CurveCrawler.load_curve_range(start_date=datetime(2022, 8, 1), end_date=datetime(2022, 8, 3), n_jobs=1)
curve = Curve(data = curve_range)

print(curve[datetime(2022, 8, 1, 12)])
curve.plot_curve(datetime(2022, 8, 1, 12))
```

</br></br>
<img src = "https://raw.githubusercontent.com/clarkmaio/NordPoolCrawler/main/img/supplydemand_example.PNG" style="width:600px;">
