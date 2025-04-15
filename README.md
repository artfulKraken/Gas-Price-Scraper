# Gas-Price-Scraper
Scrapes gas prices from AAA website for daily average gas price by California County.  Option to save data to .csv file or mysql database depending on .gasScraperConfig.yml settings

## example_.gasScraperConf File

**CHANGE FILE NAME AND PLACE IN HOME DIRECTORY**

example_.gasScraperConf.yml file name must be changed to .gasScraperConf.yml and placed in home directory of user running scraper.

- general
  - GasPriceURL: url to scrape from
  - output: `"csv"` or `"mysql"`: determines which type of output to use
- csv
  - filepath: /path/to/outputFile.csv
-mysql
  - host: host url or ip address
  - port: port of database
  - user: username
  - password: password
