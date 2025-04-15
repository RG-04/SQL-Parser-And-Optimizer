import json
import copy

def add_selects(original_json_inp, joined_json_inp):
    mapping = {}

    original_json = copy.deepcopy(original_json_inp)
    joined_json = copy.deepcopy(joined_json_inp)

    # maps tables (with aliases) to their corresponding select nodes OR table nodes
    def find_base_relations(node):
        # remove the cost attribute in node if it is present
        if node['type'] == 'select':
            # get the table inside
            if node['input']['type'] == 'base_relation':
                if 'alias' in node['input']["tables"][0]:
                    del node['input']["tables"][0]['alias']
                mapping[json.dumps(node['input'])] = node
                return
            else:
                find_base_relations(node['input'])
                return
        elif node['type'] == 'base_relation':
            if 'alias' in node["tables"][0]:
                del node["tables"][0]['alias']
            mapping[json.dumps(node)] = node
            return
        elif node['type'] == 'join':
            find_base_relations(node['left'])
            find_base_relations(node['right'])
            return
        else:
            find_base_relations(node['input'])
            return
        
    # find all base relations in the original json
    find_base_relations(original_json)

    print(json.dumps(mapping, indent=4))

    def update_join_nodes(node):
        if node['type'] == 'join':
            # get the left and right nodes
            left_node = node['left']
            right_node = node['right']
            # check if the left node is in the mapping
            if left_node['type'] == 'base_relation':
                if 'cost' in left_node:
                    del left_node['cost']
                if 'alias' in left_node["tables"][0]:
                    del left_node["tables"][0]['alias']
                print("Checking:")
                print(json.dumps(left_node, indent=4))
                if json.dumps(left_node) in mapping:
                    # get the corresponding select node
                    select_node = mapping[json.dumps(left_node)]
                    # add the select node to the left node
                    node['left'] = copy.deepcopy(select_node)
            else:
                update_join_nodes(left_node)

            if right_node['type'] == 'base_relation':
                if 'cost' in right_node:
                    del right_node['cost']
                if 'alias' in right_node["tables"][0]:
                    del right_node["tables"][0]['alias']
                print("Checking:")
                print(json.dumps(right_node, indent=4))
                if json.dumps(right_node) in mapping:
                    # get the corresponding select node
                    select_node = mapping[json.dumps(right_node)]
                    # add the select node to the right node
                    node['right'] = copy.deepcopy(select_node)
        else:
            update_join_nodes(node['input'])
            return
        
    # update the join nodes in the joined json
    update_join_nodes(joined_json)

    return joined_json


if __name__ == "__main__":
    ORIG_FILE = "optimized_out.json"
    JOINED_FILE = "best_plan.json"

    with open(ORIG_FILE, 'r') as f:
        original_json = json.load(f)
    with open(JOINED_FILE, 'r') as f:
        joined_json = json.load(f)

    # print to an out file
    with open('joined.json', 'w') as f:
        json.dump(add_selects(original_json, joined_json), f, indent=4)
        
