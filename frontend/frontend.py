import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_table
from dash.dependencies import Input, Output
import plotly
import plotly.graph_objs as go
from collections import deque
import pandas as pd
import psycopg2

app = dash.Dash(__name__)

# drop down options
brand = ['Amazon', 'Apple', 'Dell', 'Fitbit', 'GoPro', 'HP', 'Samsung']
product = ['Kindle', 'Fire TV', 'Echo Dot', 'iPhone', 'iPad', 'iPod', 'Apple TV', 'iMac', 'Apple Watch', 'AirPods', 'MacBook', 'Inspiron', 'Flex 2', 'Hero 7', 'Pavilion', 'S10']
year = [2019, 2018, 2017, 2016, 2015, 2014, 2013, 2012, 2011, 2010, 2009, 2008, 2007, 2006]
month = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]

# set page header when hover over the browser tab
app.title = 'Influence Me, Reddit!'

app.layout = html.Div(children=[
        
    html.H1(children='Please choose your brand and product from dropdown list'),
        html.Div([
            dcc.Dropdown(
                id='brands',
                options=[{'label': i, 'value': i} for i in brand],
                value='Apple',
                style={'width':'100%'}
            ),
            dcc.Dropdown(
                id='products',
                options=[{'label': i, 'value': i} for i in product],
                value='iPhone',
                style={'width':'100%'}
            ),
            dcc.Dropdown(
                id='years',
                options=[{'label': i, 'value': i} for i in year],
                value=2019,
                style={'width':'100%'}
            ),
            dcc.Dropdown(
                id='months',
                options=[{'label': i, 'value': i} for i in month],
                value=9,
                style={'width':'100%'}
            ),
        ],style={'width':'15%','display':'inline-block', 'vertical-align': 'middle', "margin-right": "6px"}),
        html.Div([
            dash_table.DataTable(
                id='table',
                columns=[
                {'name': 'Author', 'id': 'author'},
                {'name': 'Link', 'id': 'link'},
                {'name': 'Score', 'id': 'score'}],
                data=[
                {'author': i, 'link': i, 'score': i} for i in range(10)],
                style_cell={'textAlign': 'left', 'textOverflow': 'ellipsis', 'font_size': '15px'}
            )
        ],style={'width':'49%','display':'inline-block', 'vertical-align': 'middle'}),
        html.Div([
            dcc.Graph(id='graph1') 
        ],style={'width':'100%','display':'inline-block'}),
    ])

# create bar graph showing every user's score during the time period
@app.callback(Output('graph1', 'figure'),
    [
        Input(component_id='brands', component_property='value'),
        Input(component_id='products', component_property='value'),
        Input(component_id='years', component_property='value'),
        Input(component_id='months', component_property='value'),
    ])
def bar_graph(brand_value, product_value, year_value, month_value):
    # create connection from web server to database
    try:
        # ec2-xx-xxx-xxx-xxx.compute-1.amazonaws.com is the PostgreSQL server
        connection = psycopg2.connect(
            host='ec2-xx-xxx-xxx-xxx.compute-1.amazonaws.com', database='reddit', user='ubuntu')
    except Exception as e:
        sys.exit('error', e)
    
    cursor = connection.cursor()
    # query to populate the graph
    query = ("SELECT author, score FROM reddit_users_tuning WHERE year = %d AND month = %d AND brand = '%s' AND product = '%s' ORDER BY score DESC" % (year_value, month_value, brand_value, product_value))
    cursor.execute(query)
    rows = cursor.fetchall()

    # create dataframe to hold query result for the bar graph
    labels = ['author','score']
    df = pd.DataFrame.from_records(rows, columns=labels)

    data = go.Bar(
        x = df['author'].tolist(),
        y = df['score'].tolist(),
        name="bar")
    
    layout = go.Layout(
        title= "Score graph of users commented about %s %s during this period" % (brand_value, product_value)
    )
    
    return {'data': [data],'layout' : layout}

# create table showing the top 10 influencers, their scores, and link to their profiles (can be used as contact info)
@app.callback(Output('table', 'data'),
    [
        Input(component_id='brands', component_property='value'),
        Input(component_id='products', component_property='value'),
        Input(component_id='years', component_property='value'),
        Input(component_id='months', component_property='value')
    ])
def reddit_table(brand_value, product_value, year_value, month_value):
    # create connection from web server to database
    try:
        # ec2-xx-xxx-xxx-xxx.compute-1.amazonaws.com is the PostgreSQL server
        connection = psycopg2.connect(host='ec2-xx-xxx-xxx-xxx.compute-1.amazonaws.com', database='reddit', user='ubuntu')
    except Exception as e:
        sys.exit('error', e)
    
    cursor = connection.cursor()
    # query to populate the table
    query = ("SELECT author, score FROM reddit_users_tuning WHERE year = %d AND month = %d AND brand = '%s' AND product = '%s' ORDER BY score DESC LIMIT 10" % (year_value, month_value, brand_value, product_value))
    cursor.execute(query)
    rows = cursor.fetchall()

    # create dataframe to hold query result for the table
    labels = ['author','score']
    df = pd.DataFrame.from_records(rows, columns=labels)
    df['link'] = 'https://www.reddit.com/user/' + df['author']
    df = df[['author', 'link', 'score']]

    return df.to_dict(orient='records')

if __name__ == "__main__":
    # EC2 instance's default HTTP port is 80
    app.run_server(host="0.0.0.0", port=80, debug=True)