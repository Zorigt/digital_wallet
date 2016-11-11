Python libs used sys, time, copy

Must run in this order:
- import_batch_input(...)
- feature_1(...)
- feature_2(...)
- feature_3(...)

As long as you just run the antifraud.py file, there shouldn't be any issues. The reason for running one after the other is because they are dependent on each other for optimization. 

batch file is uploaded into dictionary graph with keys as id and list of user ids as value.
For example: graph_batch = {id1: [id2,id3], id2: [id1], id3: [id1]} 

feature 1 uses simple look up in dictionary graph

feature 2 takes two lists from each id key and takes intersection. If they intersect then it is trusted otherwise unverified. Note: 
feature 2 re-uses the output_list of trusted/unverified from feature 1. 

feature 3 uses breach first bidirectional search to the 3rd degree on each side. Note: feature 3 re-uses the output_list of trusted/unverified from feature 2. 

Here are my run times:

loading batch_payment.txt takes 25 seconds
feature 1 takes 35 seconds
feature 2 takes 38 seconds
feature 3 takes 460 seconds

A couple of ideas for other fraud detection, but didn't implement:
- Check if payment exceeds certain amount. For example: people are more likely to transfer small amounts like for coffee, lunch, and movie tickets, etc. So, if any any amount exceeds abvoe $80 then it could be labaled unverified. 
- Detect unusual frequency of money transfers within specific time persiod. For exmaple: if there were multiple transactions within 4 hour period then it could be labeled unverified as people will most likely to transfer once a day or even a week. 

Zorigt
