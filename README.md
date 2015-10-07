# wikipedia-power-plants

This is code for downloading all the articles on Wikipedia about power plants

# Why is this interesting?

1. If you look at https://en.wikipedia.org/wiki/Category:Power_stations_by_country, you'll find the top-level category of all the power plants.  This code, via a single query to DBpedia is able to perform a hierarchical traversal over this entire structure to retrieve 4000+ articles.
2. Geographic coordinates are extracted as well
3. **TODO:** data stored in tables, such as https://en.wikipedia.org/wiki/List_of_power_plants_in_the_Philippines are processed too.
4. **In progress:** a record of article revision ids is kept, which allows for keeping the data up to date.

With a little extra effort, this can be used to:

1. Alert you about new or controversial projects (check for new articles, large numbers of revisions, edit wars, etc.).  See https://en.wikipedia.org/wiki/Jaitapur_Nuclear_Power_Project for an example.
2. Cross-check other databases about power plants.  Many of the power plant articles contain references, and if there are any discrepancies, you just need to read the links to figure out what's actually going on.
