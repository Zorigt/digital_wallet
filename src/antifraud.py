#! /usr/bin/env python
# Zorigt Baz
# zorigt.baz@gmail.com

import sys, time, copy

# Use global output_list of trusted and unverified to reuse the results 
# like so feature 1 -> feature 2 -> feature 3
output_list = []

def main():

  # In and out file paths
  batch_input, stream_input = sys.argv[1], sys.argv[2]
  output_1, output_2, output_3 = sys.argv[3], sys.argv[4], sys.argv[5]

  print "Importing " + str(batch_input) + " ..."
  start0 = time.time()
  graph_batch_1 = import_batch_input(batch_input)
  end0 = time.time()
  print "Processed " + str(batch_input) + " in " + str(end0 - start0) + " seconds"

  graph_batch_2 = copy.deepcopy(graph_batch_1)
  graph_batch_3 = copy.deepcopy(graph_batch_1)

  print "Processing stream_payment.txt for Feature Numero Uno ..."
  start1 = time.time()
  feature_1(output_1, stream_input, graph_batch_1)
  end1 = time.time()
  print "Feature One completed in " + str(end1 - start1) + " seconds"

  print "Processing stream_payment.txt for Feature Numero Dos ..."
  start2 = time.time()
  feature_2(output_2, stream_input, graph_batch_2)
  end2 = time.time()
  print "Feature Two completed in " + str(end2 - start2) + " seconds"
 
  print "Processing stream_payment.txt for Feature Numero Tres ..."
  start3 = time.time()
  feature_3(output_3, stream_input, graph_batch_3)
  end3 = time.time()  
  print "Feature Three completed in " + str(end3 - start3) + " seconds"


def record_transaction(id1, id2, graph, id1_list):
  
  if id1_list:
    if id2 not in id1_list:
      graph[id1].append(id2)
      return ['unverified', graph]
    else:
      return ['trusted', graph]
  else:
    graph[id1] = [id2]
    return ['unverified', graph]


def check_records(id1, graph):
  
  id1_list = graph.get(id1)
  
  if id1_list:
    return id1_list
  else:
    return id1_list


def import_batch_input(batch_input_path):
  # it populates dictionary graph_batch with user id as key and a list of user ids as value, 
  # ie graph_batch = {id1: [id2,id3], id2: [id1], id3: [id1]}  

  graph_batch = {}

  with open(batch_input_path, 'rU') as read_from:
    batch = read_from.read().split("\n") 

    count = 0

    for row in batch:
      count += 1
      i = row.split(',')

      #validate each row by timestamp exists as the first item 
      timestamp = i[0]
      if len(timestamp) != 19 and len(timestamp.split('-')) != 3:
        continue

      id1 = int(i[1].strip(' '))
      id2 = int(i[2].strip(' '))

      #if there is record then it return a list of associated ids
      id1_list = check_records(id1, graph_batch)
      #record_transaction returns an 2 length array, ie [trusted, graph]
      id1_array = record_transaction(id1, id2, graph_batch, id1_list)
      graph_batch = id1_array[1]
      
      if id1_array[0] == 'unverified':
        id2_list = check_records(id2, graph_batch)
        graph_batch = record_transaction(id2, id1, graph_batch, id2_list)[1]
    
    read_from.close()
    return graph_batch


def feature_1(output_path, stream_input_path, graph_batch):
  # just read each row in txt file and check it against the graph_batch

  with open(stream_input_path, 'rU') as read_from:
    stream = read_from.readlines() 

    for row in stream:
      i = row.split(',')

      #validate each row by timestamp exists as the first item 
      timestamp = i[0]
      if len(timestamp) != 19 and len(timestamp.split('-')) != 3:
        continue

      id1 = int(i[1].strip(' '))
      id2 = int(i[2].strip(' '))

      #if there is record then it return a list of associated ids
      id1_list = check_records(id1, graph_batch)
      #record_transaction returns an 2 length array, ie [trusted, graph]
      id1_array = record_transaction(id1, id2, graph_batch, id1_list)
      output_list.append(id1_array[0])
      graph_batch = id1_array[1]

      if id1_array[0] == 'unverified':
        id2_list = check_records(id2, graph_batch)
        graph_batch = record_transaction(id2, id1, graph_batch, id2_list)[1]
  
    read_from.close()

  with open(output_path, "w") as write_to:
    write_to.write("\n".join(output_list))


def feature_2(output_path, stream_input_path, graph_batch):
  # get the associated id lists for id1 and id2 and then take the intersection of the two lists

  count = 0  
  with open(stream_input_path, 'rU') as read_from:
    stream = read_from.readlines() 

    for row in stream:

      found = False
      i = row.split(',')

      #validate each row by timestamp exists as the first item 
      timestamp = i[0]
      if len(timestamp) != 19 and len(timestamp.split('-')) != 3:
        continue

      # use the result from feature 1 and only check the unverified items
      count += 1
      if output_list[count-1] == 'unverified':

        id1 = int(i[1].strip(' '))
        id2 = int(i[2].strip(' '))
        
        id1_list = check_records(id1, graph_batch)
        id1_array = record_transaction(id1, id2, graph_batch, id1_list)
        graph_batch = id1_array[1]
        
        if id1_array[0] == 'unverified':
          id2_list = check_records(id2, graph_batch)
          id2_array = record_transaction(id2, id1, graph_batch, id2_list)
          graph_batch = id2_array[1]

          if id1_list == None or id2_list == None:
            found = False
            #output_list.append('unverified')
          else:
            intersect_set_id1_id2_lists = set(id1_list).intersection(id2_list)
            if intersect_set_id1_id2_lists:
              found = True
              #output_list.append('trusted')
            else:
              found = False
              #output_list.append('unverified')

        else:
          found = True

        if found:
          output_list[count-1] = 'trusted'
          found = False
    #count += 1

    read_from.close()

  with open(output_path, "w") as write_to:
    write_to.write("\n".join(output_list))
    write_to.close()


def feature_3(output_path, stream_input_path, graph_batch):
  # Breadth-first bidirectional search on id1 and id2

  count = 0
  
  with open(stream_input_path, 'rU') as read_from:
    stream = read_from.readlines() 

    for row in stream:

      found = False
      i = row.split(',')
      timestamp = i[0]
      
      # validate each row by timestamp exists as the first item 
      if len(timestamp) != 19 and len(timestamp.split('-')) != 3:
        continue
      
      # use the result from feature 2 and only check the unverified items
      count += 1
      if output_list[count-1] == 'unverified':

        if count % 1000 == 0:
          print str(count) + " counter is counting ..."

        id1 = int(i[1].strip(' '))
        id2 = int(i[2].strip(' '))
        
        id1_list = check_records(id1, graph_batch)
        id2_list = check_records(id2, graph_batch)

        if id1_list == None or id2_list == None:
          found = False

        else:         
          user_list1, node_list = [], []
          for user1 in id1_list:
            node_list = check_records(user1, graph_batch)
            
            user_list2 = []
            for user2 in id2_list:
              
              #check if 3rd node exsists in 2nd node's list
              if user2 in node_list:
                found = True
                break
              
              user_list1 += node_list
              user_list2 += check_records(user2, graph_batch)
            
            if found:
              break

          if found == False:
            if set(user_list1).intersection(user_list2):
              found = True
        
        if found:
          output_list[count-1] = 'trusted'

        # Record the transcation between id1 and id2
        graph_batch = record_transaction(id1, id2, graph_batch, id1_list)[1]
        graph_batch = record_transaction(id2, id1, graph_batch, id2_list)[1]
      
    read_from.close()

  with open(output_path, "w") as write_to:
    write_to.write("\n".join(output_list))
    write_to.close()


if __name__ == "__main__":
  main()