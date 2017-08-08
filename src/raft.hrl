-record(metadata, {
            name,
            nodes,
            term,
            votes = [],
            voted_for = null,
            leader_id = null
           }).


-record(client_message, {}).
