# Learn about API authentication here: https://plot.ly/python/getting-started
# Find your api_key here: https://plot.ly/settings/api

import plotly.plotly as py
from plotly.graph_objs import *

"""
# synthetic 1, latency
trace_before = Bar(
    x=['Greedy', 'Volley', 'Barrage'],
    y=[2.485819909, 2.485819909, 2.485819909],
    name='before'
)
trace_after = Bar(
    x=['Greedy', 'Volley', 'Barrage'],
    y=[1.211504,1.211504,1.211504],
    name='after'
)
data = Data([trace_before, trace_after])
layout = Layout(
    barmode='group'
)
fig = Figure(data=data, layout=layout)
plot_url = py.plot(fig, filename='syn1latency')


# synthetic 1, cost
trace_cost = Bar(
    x=['Greedy', 'Volley', 'Barrage'],
    y=[2.485819909, 2.485819909, 2.485819909],
    name='before'
)
data = Data([trace_cost])
layout = Layout(
    barmode='group'
)
fig = Figure(data=data, layout=layout)
plot_url = py.plot(fig, filename='syn1cost')

# synthetic 2, latency
trace_before = Bar(
    x=['Greedy', 'Volley', 'Barrage'],
    y=[1.8681, 1.8681, 1.8681],
    name='before'
)
trace_after = Bar(
    x=['Greedy', 'Volley', 'Barrage'],
    y=[0.824431,1.8548784,0.824431],
    name='after'
)
data = Data([trace_before, trace_after])
layout = Layout(
    barmode='group'
)
fig = Figure(data=data, layout=layout)
plot_url = py.plot(fig, filename='syn2lat')


# twitter, latency
trace_before = Bar(
    x=['Greedy', 'Volley', 'Barrage'],
    y=[8.51223259827, 8.51223259827, 8.51223259827],
    name='before'
)
trace_after = Bar(
    x=['Greedy', 'Volley', 'Barrage'],
    y=[3.28863798156,4.42407719477,4.12398404749],
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
    x=['Greedy', 'Volley', 'Barrage'],
    y=[1167, 1148, 1181],
    name='before'
)
data = Data([trace_cost])
layout = Layout(
    barmode='group'
)
fig = Figure(data=data, layout=layout)
plot_url = py.plot(fig, filename='twittercost')


# twitter, latency
trace_before = Bar(
    x=['Greedy', 'Volley', 'Barrage'],
    y=[-61.36574108, -48.02682911, -51.55226317],
    name='Randomized'
)
trace_after = Bar(
    x=['Greedy', 'Volley', 'Barrage'],
    y=[-5.662519285, 4.333506341, 4.671667928],
    name='Optimized'
)
data = Data([trace_before, trace_after])
layout = Layout(
    barmode='group'
)
fig = Figure(data=data, layout=layout)
plot_url = py.plot(fig, filename='twitterrandom_correct')

# twitter, cost
trace_cost = Bar(
    x=['Greedy', 'Volley', 'NewAlgo'],
    y=[1167, 1148, 1181],
    name='before'
)
data = Data([trace_cost])
layout = Layout(
    barmode='group'
)
fig = Figure(data=data, layout=layout)
plot_url = py.plot(fig, filename='twittercost')
"""

# synthetic 4, latency
trace_before = Bar(
    x=['Greedy', 'Volley', 'Barrage'],
    y=[1.101829927,1.101829927,1.101829927],
    name='before'
)
trace_after = Bar(
    x=['Greedy', 'Volley', 'Barrage'],
    y=[1.101829927,1.105380462,1.105380462],
    name='after'
)
data = Data([trace_before, trace_after])
layout = Layout(
    barmode='group'
)
fig = Figure(data=data, layout=layout)
plot_url = py.plot(fig, filename='syn4latency')

