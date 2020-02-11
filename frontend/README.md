## To run the front-end server

Execute the `frontend.py` with root permission:

`sudo python3 frontend.py`

Requirements:
- Python3
- Flask
- Dash
- Pandas
- Psycopg2

## How to use the demo website

You can choose the `brand`, corresponding `product`, `year`, and `month` from the drop down list. The resulting table is the top 10 Reddit influencers associated with your brand and product during the month that you choose. The `Link` column shows the Reddit user profile where you can send the user a message. The `Score` column measures the net number of people agree with poster. The score graph of all users is also displayed.