# Learn about API authentication here: https://plot.ly/python/getting-started
# Find your api_key here: https://plot.ly/settings/api

import plotly.plotly as py
from plotly.graph_objs import *
"""
# synthetic 1, latency
trace_before = Bar(
    x=['Greedy', 'Volley', 'NewAlgo'],
    y=[2.485819909, 2.485819909, 2.485819909],
    name='before'
)
trace_after = Bar(
    x=['Greedy', 'Volley', 'NewAlgo'],
    y=[1.211504,1.211504,1.211504],
    name='after'
)
data = Data([trace_before, trace_after])
layout = Layout(
    barmode='group'
)
fig = Figure(data=data, layout=layout)
plot_url = py.plot(fig, filename='syn1lat')


# synthetic 1, cost
trace_cost = Bar(
    x=['Greedy', 'Volley', 'NewAlgo'],
    y=[2.485819909, 2.485819909, 2.485819909],
    name='before'
)
data = Data([trace_cost])
layout = Layout(
    barmode='group'
)
fig = Figure(data=data, layout=layout)
plot_url = py.plot(fig, filename='syn1cost')
"""
"""
# synthetic 2, latency
trace_before = Bar(
    x=['Greedy', 'Volley', 'NewAlgo'],
    y=[1.8681, 1.8681, 1.8681],
    name='before'
)
trace_after = Bar(
    x=['Greedy', 'Volley', 'NewAlgo'],
    y=[0.824431,1.8548784,0.824431],
    name='after'
)
data = Data([trace_before, trace_after])
layout = Layout(
    barmode='group'
)
fig = Figure(data=data, layout=layout)
plot_url = py.plot(fig, filename='syn2lat')
"""

# twitter, latency
trace_before = Bar(
    x=['Greedy', 'Volley', 'NewAlgo'],
    y=[4.27882670241, 4.27882670241, 4.27882670241],
    name='before'
)
trace_after = Bar(
    x=['Greedy', 'Volley', 'NewAlgo'],
    y=[4.03653731521,5.30286557481,6.92971383811],
    name='after'
)
data = Data([trace_before, trace_after])
layout = Layout(
    barmode='group'
)
fig = Figure(data=data, layout=layout)
plot_url = py.plot(fig, filename='twitterlat')


# twitter, cost
trace_cost = Bar(
    x=['Greedy', 'Volley', 'NewAlgo'],
    y=[47, 509, 912],
    name='before'
)
data = Data([trace_cost])
layout = Layout(
    barmode='group'
)
fig = Figure(data=data, layout=layout)
plot_url = py.plot(fig, filename='twittercost')

