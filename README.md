This repository contains three key scripts used to create The Flatline Gallery. 

dune_query_input is the sql query for identifying the full list of large-scale eth nft collections from feb 2021 - sep 2023 
that likely experienced a full-life cycle, using Dune Analytics.

This list was used as an input for the socials_nfts.py script, which links the twitter handle and discord invite link for each collection
using the free opeansea api.

Finally, the output from the socials_nfts.py script was fed into nft_death_Score_analyzer.py, which scores each collection across five categories using active market data to confirm whether it is actually dead. 
