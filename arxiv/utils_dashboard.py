def sizes_to_rowcols(l):
	row_col = []
	for i in range(len(l)):
		if i == 0:
			row_col.append([1,1])
		elif i % 2 == 1:
			row_col.append([None,row_col[i-1][1]+l[i-1][0]])
		else:
			row_col.append([None,1])

	for i in range(len(l)):
		if i == 0:
			continue
		elif i % 2 == 1:
			row_col[i][0] = row_col[i-1][0]
		else:
			row_col[i][0] = row_col[i-1][0] + l[i-1][1]
	return row_col

def dashboard(config):
	dashboard_dict = {}
	dashboard_dict['_id'] = config['dashboard_id_title']
	dashboard_dict['_type'] = "dashboard"
	_source = {}
	_source['title'] = config['dashboard_id_title']
	_source["hits"] =  0
	_source["description"] = ""
	panelsJson = "["
	for i in range(config['n_visu']):
		panelsJson += '{\"id\":\"' + config['visu_titles'][i]
		panelsJson += '\",\"panelIndex\":{}'.format(i+1)
		panelsJson += ',\"row\":{}'.format(config['rowcol'][i][0])
		panelsJson += ',\"col\":{}'.format(config['rowcol'][i][1])
		panelsJson +=',\"size_x\":{}'.format(config['visu_sizes'][i][0])
		panelsJson += ',\"size_y\":{}'.format(config['visu_sizes'][i][1])
		panelsJson += ',\"type\":\"visualization\"}'

		if i != config['n_visu']-1:
			panelsJson+=","
		else:
			panelsJson+="]"

	_source['panelsJSON'] = panelsJson
	_source['optionsJSON'] = '{\"darkTheme\":'+ config['darktheme'] + '}'
	_source['uiStateJSON'] = '{}'
	_source['version'] = 1
	_source['timeRestore'] = True
	_source['timeTo'] = config['timeTo']
	_source['timeFrom'] = config['timeFrom']
	_source['kibanaSavedObjectMeta'] = {}
	_source['kibanaSavedObjectMeta']['searchSourceJSON'] = '{\"filter\":[{\"query\":{\"query_string\":{\"analyze_wildcard\":true,\"query\":\"*\"}}}]}'

	dashboard_dict['_source'] = _source

	return dashboard_dict

def regvisu(config, i):
	visu = {}
	visu["_id"] = config['visu_titles'][i]
	visu["_type"] = "visualization"
	_source = {}
	_source['title'] = config['visu_titles'][i]
	_source['uiStateJSON'] = '{\"spy\":{\"mode\":{\"name\":null,\"fill\":false}}}'
	_source['description'] = ""
	_source['version'] = 1
	_source['kibanaSavedObjectMeta'] = {}
	_source['kibanaSavedObjectMeta']['searchSourceJSON'] = '{\"index\":\"'+config['index']+'\",\"query\":{\"query_string\":{\"analyze_wildcard\":true,\"query\":\"*\"}},\"filter\":[]}'
	visState = ''
	visState += '{\"title\":\"'+config['visu_titles'][i]+'\",\"type\":\"line\",'
	visState += '\"params\":{\"addLegend\":true,\"addTimeMarker\":true,\"addTooltip\":true,\"defaultYExtents\":true,\"drawLinesBetweenPoints\":true,\"interpolate\":\"linear\",\"radiusRatio\":9,\"scale\":\"linear\",\"setYExtents\":false,\"shareYAxis\":true,\"showCircles\":true,\"smoothLines\":true,\"times\":[],\"yAxis\":{}},\"aggs\":['
	visState += '{\"id\":\"1\",\"type\":\"avg\",\"schema\":\"metric\","params\":{\"field\":\"'+config['fields'][i]+'\",\"customLabel\":\"'+config['plot_titles'][i]+'\"}},'
	visState += '{\"id\":\"2\",\"type\":\"max\",\"schema\":\"radius\",\"params\":{\"field\":\"'+config['scores'][i]+'\"}},'
	visState += '{\"id\":\"3\",\"type\":\"date_histogram\",\"schema\":\"segment\",\"params\":{\"field\":\"time\",\"interval\":\"custom\",\"customInterval\":\"5s\",\"min_doc_count\":1,\"extended_bounds\":{}}}],'
	visState += '\"listeners\":{}}'
	_source['visState'] = visState
	visu["_source"] = _source

	return visu

def spidervisu(config, i):
	others = config['fields'][:]
	others.pop(i)

	visu = {}
	visu["_id"] = config['visu_titles'][i]
	visu["_type"] = "visualization"
	_source = {}
	_source['title'] = config['visu_titles'][i]
	_source['uiStateJSON'] = '{}'
	_source['description'] = ""
	_source['version'] = 1
	_source["kibanaSavedObjectMeta"] = {}
	_source["kibanaSavedObjectMeta"]['searchSourceJSON'] = "{\"index\":\""+config['score_index']+"\",\"query\":{\"query_string\":{\"analyze_wildcard\":true,\"query\":\"*\"}},\"filter\":[]}"
	visState = "{\"title\":\"' + config['visu_titles'][i] + '\",\"type\":\"radar\",\"params\":{\"addAxe\":true,\"addAxeLabel\":true,\"addLabelScale\":0.9,\"addLegend\":true,\"addLevel\":true,\"addLevelLabel\":true,\"addLevelNumber\":5,\"addLevelScale\":1,\"addPolygon\":true,\"addTooltip\":true,\"addVertice\":true,\"fontSize\":60,\"isDonut\":false,\"isFacet\":false,\"shareYAxis\":true},\"aggs\":["
	for j in range(len(others)):
		visState += '{\"id\":\"'+str(j)+'\",\"type\":\"avg\",\"schema\":\"metric\",\"params\":{\"field\":\"'+others[j]+'\"}},'
	visState += '{\"id\":\"'+str(len(others)+1)+'\",\"type\":\"terms\",\"schema\":\"segment\",\"params\":{\"field\":\"name\",\"size\":5,\"order\":\"desc\",\"orderBy\":\"_term\"}}],\"listeners\":{}}'
	_source['visState'] = visState
	visu['_source'] = _source

	return visu












