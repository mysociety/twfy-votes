# TheyWorkForYou Votes

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/mysociety/twfy-votes)

Experimental approach to powering TheyWorkForYou voting records and removing reliance on the Public Whip. It provides several views to replicate the current feeds from Public Whip into TheyWorkForYou. 

This is a FastAPI app that reprocesses the Public Whip database dumps so the calculations over the top of the raw data can be modified.

The goal is that this app could later be expanded with a better data management interface, or that the analytic functions could be lifted into a 
django approach. 

The raw Public Whip data is currently held at https://pages.mysociety.org/publicwhip-data/. In the long run, we would want to replace this with something feeding directly off parlparse. 

The database is held in memory through duckdb for ease of processing the analytic queries. This would need to be augmented with a better approach when moving policy storage out of yaml files. 

In principle, this current version can be deployed as a static site (not yet configured because of long generation time for 12,000 divisions).

# Running

- `script/update` -- generates some calculated tables required before running.
- `script/server` -- should start a webserver on localhost:8000.

For more options `python -m twfy-votes --help`.

# Development

There's a configured devcontainer for working in VS Code otherwise - the `dockerfile.dev` container is the one to use. As duckdb is used, there's no current need for separate DB host.

Code formatting is done via ruff: `ruff format .`.

## Models

Models are roughly divided between the `decisions` and `policies` apps.

- Decisions are conceptually slightly wider than 'divisions' and there are draft data structures for handing 'agreements' (decisions taken without a vote).
- Policies are collections of votes that relate to individuals. 

Pydantic models are used for ensuring validity when moving things in and out of database queries. 

Partial models are defined for when the reference to an object is available (e.g. the unique information for a division) - but it is not itself the full version. 

Generally this follows the thinking that everything passed between functions should itself be a clearly defined model (rather than a tuple or dict) - so helper models exist to group associated items together for views. 

## Divergence from Public Whip and TheyWorkForYou approach

The ultimate goal is to be able to safely change our voting records approach with good cross-testing of the underlying process. This app impliments two parellel methods of converting voting information for policies into 'vote distributions' (number of strong, weak, absent votes) to check the faster SQL based approach matches the more step-by-step python approach. 

The key planned divergence is on the contents of the policies (which can now be changed and tracked from the yaml files) and in the process to convert distributions to scores. This is customisable on a per policy basis, but this hasn't been turned on yet. 

The only divergence in the new implementation that a Policy is associated with a specific chamber - and will ignore (and we should eventually remove) divisions that take part in other chambers. This leads to some small divergences with current data in Public Whip where someone has been both an MP and a Lord/MSP and there are relevant voting lines also in those chambers. 


## Database and queries

Because of not configuring a database normally, more stuff has to go through basic SQL that is ideal (but this is very useful for more analytic queries).

The `data_sources.py` files configure the basic sources, tables and views available to the app. Anything decorated with `as_cached_table` will be rendered to a parquet file when `script/update` is used. If that hasn't been run, they will be stored as views (slower).

`queries.py` store queries to retrieve specific information for creating models or views. These use jinja formatting to define variables for use - which are made safe using [jinjasql](https://github.com/sripathikrishnan/jinjasql) later in the process. 

The pattern for passing objects to a view is usually:

- A query retrieves the basic information.
- This is used by an async factory object in the model to create model instances. 
- A fastapi dependency chain will construct an object.
- The dependency is references in multiple views (to support api and human views with common logic)