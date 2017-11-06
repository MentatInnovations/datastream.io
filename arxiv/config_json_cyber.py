import json
import utils

'''
README:

This is a script to generate dashboard for kibana demos.

User can only modify what is in the TO MODIFY section.

Please note:

There are 2 kinds of plots supported: regular line plots and spider plots.

For now, user can only use ONE spider plot.

If you are to use a spider plot, please use the word spider in the visu-title section, and fill in None at the exact same position in the plot_titles, fields and score sections.

Spider plots use fields to gather information. If you are to plot information that is not contained in its own visualisation, that's is not an issue as long as you add the unplotted fields
at the end of the fields list. e.g. 'brake_pedal_status' is not contained in a visualisation on its own, it doesn't even have a score associated, still it will appear in the spider plot.

PLEASE enter the sizes in the order you used for the other lists.

It will be plotted as:

0 1
2 3
4 5
6 7
etc

where i is the index of an element in the list.
'''
#### config
config = {}

#### TO MODIFY:
config['dashboard_id_title'] = "cyber"
config['n_visu'] = 6
config['visu_titles'] = ['duration', 'spider', 'no_events', 'user_agent_matching', 'request_regex_matching', 'new_ips']
config['plot_titles'] = ['Duration', None, 'Number of Events', 'User Agent Matching', 'Request Regex Matching', 'New IPs']
config['fields'] = ['duration', None, 'no_events', 'user_agent_matching', 'request_regex_matching', 'new_ips']
config['scores'] = ['SCORES_duration', None, 'SCORES_no_events', 'SCORES_user_agent_matching', 'SCORES_request_regex_matching', 'new_ips']

config['visu_sizes'] = [(7,4),(5,4),(6,3),(6,3),(6,3),(6,3)]
config['darktheme'] = 'false'
config['timeTo'] = 'now'
config['timeFrom'] = 'now-15m'
config['index'] = 'tele-full'
config['score_index'] = 'tele-scores'

#### NOT TO MODIFY:
config['rowcol'] = utils.sizes_to_rowcols(config['visu_sizes'])

#### Initialisation
js = []
dashboard = utils.dashboard(config)
js.append(dashboard)

for i in range(config['n_visu']):
# for i in range(1,2):
	if 'spider' not in config['visu_titles'][i]:
		js.append(utils.regvisu(config,i))
	else:
		js.append(utils.spidervisu(config,i))

with open('dashboard.json', 'w') as fp:
    json.dump(js, fp, sort_keys=True)








