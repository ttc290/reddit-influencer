import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_table
from dash.dependencies import Input, Output
import plotly
import plotly.graph_objs as go
import pandas as pd
import psycopg2

app = dash.Dash(__name__)

app.title = 'Influence Me, Reddit!'
app.layout = html.Div(children=[
        
    html.H1(children='Choose your brand & product from dropdown on the left'),
        html.Div([
            dcc.Dropdown(
                id='brands',
                value='Apple',
                style={'width':'100%'}
            ),
            dcc.Dropdown(
                id='products',
                value='iPhone',
                style={'width':'100%'}
            ),
            dcc.Dropdown(
                id='years',
                options=[{'label': i, 'value': i} for i in range(2019, 2005, -1)],
                value=2019,
                style={'width':'100%'}
            ),
            dcc.Dropdown(
                id='months',
                options=[{'label': i, 'value': i} for i in range(1, 13)],
                value=9,
                style={'width':'100%'}
            ),
        ],style={'width':'20%','display':'inline-block', 'vertical-align': 'left', "margin-right": "56px"}),
        html.Div([
            dash_table.DataTable(
                id='table',
                columns=[
                {'name': 'Author', 'id': 'author'},
                {'name': 'User\'s Profile', 'id': 'link'},
                {'name': 'Total Score', 'id': 'score'}],
                data=[
                {'author': i, 'link': i, 'score': i} for i in range(10)],
                style_header={'backgroundColor': 'yellow', 'fontWeight': 'bold'},
                style_cell={'textAlign': 'left', 'font_size': '15px', 'height': 'auto', 'whiteSpace': 'normal'}
            )
        ],style={'width':'49%','display':'inline-block', 'vertical-align': 'middle'}),
        html.Div([
            dcc.Graph(id='graph') 
        ],style={'width':'100%','display':'inline-block', "margin-bottom": "16px"}),
        html.Div([
            dash_table.DataTable(
                id='comment_table',
                columns=[
                {'name': 'Author', 'id': 'author'},
                {'name': 'Top Comment', 'id': 'comment'},
                {'name': 'Sentiment', 'id': 'sentiment'},
                {'name': 'Score', 'id': 'score'}],
                data=[
                {'author': i, 'comment': i, 'sentiment': i, 'score': i} for i in range(20)],
                style_header={'backgroundColor': 'yellow', 'fontWeight': 'bold'},
                style_cell={'textAlign': 'left', 'font_size': '15px', 'height': 'auto', 'whiteSpace': 'normal'}
            )
        ],style={'width':'90%','display':'inline-block', 'vertical-align': 'left'}),
    ])

# database connection configurations
db = {'host':'ec2-xx-xxx-xxx-xxx.compute-1.amazonaws.com', 'database': 'xxxxxx', 'user': 'xxxxxx'}

# populate brand options
@app.callback(Output('brands', 'options'),
    [
        Input(component_id='brands', component_property='value')
    ])
def brand_options(brand_value):
    try:
        connection = psycopg2.connect(**db)
    except Exception as e:
        sys.exit('error', e)

    cursor = connection.cursor()
    query = ("SELECT DISTINCT brand FROM brand_product ORDER BY brand ASC")
    cursor.execute(query)
    brand_list = cursor.fetchall()
    brand = [i[0] for i in brand_list]
    cursor.close()
    connection.close()
    
    return [{'label': i, 'value': i} for i in brand]

# populate product options
@app.callback(Output('products', 'options'),
    [
        Input(component_id='products', component_property='value')
    ])
def product_options(product_value):
    try:
        connection = psycopg2.connect(**db)
    except Exception as e:
        sys.exit('error', e)

    cursor = connection.cursor()
    query = ("SELECT DISTINCT product FROM brand_product ORDER BY product ASC")
    cursor.execute(query)
    product_list = cursor.fetchall()
    product = [i[0] for i in product_list]
    cursor.close()
    connection.close()
    
    return [{'label': i, 'value': i} for i in product]

# populate user score's graph
@app.callback(Output('graph', 'figure'),
    [
        Input(component_id='brands', component_property='value'),
        Input(component_id='products', component_property='value'),
        Input(component_id='years', component_property='value'),
        Input(component_id='months', component_property='value'),
    ])
def update_graph_bar(brand_value, product_value, year_value, month_value):
    try:
        connection = psycopg2.connect(**db)
    except Exception as e:
        sys.exit('error', e)
    
    cursor = connection.cursor()
    query = ("SELECT author, score FROM reddit_users_score WHERE year = %d AND month = %d AND brand = '%s' AND product = '%s' ORDER BY score DESC" % (year_value, month_value, brand_value, product_value))
    cursor.execute(query)
    rows = cursor.fetchall()

    labels = ['author','score']
    df = pd.DataFrame.from_records(rows, columns=labels)

    data = go.Bar(
        x = df['author'].tolist(),
        y = df['score'].tolist(),
        name="bar")
    
    layout = go.Layout(
        title= "Score graph of users commented about %s %s during this period" % (brand_value, product_value),
        xaxis={'tickangle': 45},
        #yaxis={'title': 'Aggregated score'}
    )

    cursor.close()
    connection.close()
    
    return {'data': [data],'layout' : layout}

# populate top users' table
@app.callback(Output('table', 'data'),
    [
        Input(component_id='brands', component_property='value'),
        Input(component_id='products', component_property='value'),
        Input(component_id='years', component_property='value'),
        Input(component_id='months', component_property='value')
    ])
def reddit_table(brand_value, product_value, year_value, month_value):
    try:
        connection = psycopg2.connect(**db)
    except Exception as e:
        sys.exit('error', e)
    
    cursor = connection.cursor()
    query = ("SELECT author, score FROM reddit_users_score WHERE year = %d AND month = %d AND brand = '%s' AND product = '%s' ORDER BY score DESC LIMIT 10" % (year_value, month_value, brand_value, product_value))
    cursor.execute(query)
    rows = cursor.fetchall()

    labels = ['author','score']
    df = pd.DataFrame.from_records(rows, columns=labels)
    df['link'] = 'https://www.reddit.com/user/' + df['author']
    df = df[['author', 'link', 'score']]

    cursor.close()
    connection.close()

    return df.to_dict(orient='records')

# populate top users' comment
@app.callback(Output('comment_table', 'data'),
    [
        Input(component_id='brands', component_property='value'),
        Input(component_id='products', component_property='value'),
        Input(component_id='years', component_property='value'),
        Input(component_id='months', component_property='value')
    ])
def comment_table(brand_value, product_value, year_value, month_value):
    try:
        connection = psycopg2.connect(**db)
    except Exception as e:
        sys.exit('error', e)
    
    cursor = connection.cursor()
    query = ("SELECT author, maxScore, body, sentiment FROM master_table WHERE year = %d AND month = %d AND brand = '%s' AND product = '%s' ORDER BY score DESC LIMIT 10" % (year_value, month_value, brand_value, product_value))
    cursor.execute(query)
    rows = cursor.fetchall()

    labels = ['author', 'score', 'comment', 'sentiment']
    df = pd.DataFrame.from_records(rows, columns=labels)
    
    cursor.close()
    connection.close()

    return df.to_dict(orient='records')

if __name__ == "__main__":
    app.run_server(host="0.0.0.0", port=80, debug=True)
