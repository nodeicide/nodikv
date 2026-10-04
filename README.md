# NODIKV
#### by [nodeicide](https://nodeicide.com)

### K/V Distributed Database ->
    Current version uses iroh to connect the nodes,
    you must run the bootstrap and copy the endpoint ID into the node.rs ID
    
    when you run you can choose between two commands
        `PULL k` -> read
        `PUSH k v` -> write 
    THIS IS NOT CURRENTLY CONSISTENT FOR SURE and is more about availability in current design  

    you can run as many nodes as you want as long as they connect to the correct 
    bootstrap ID
