Neo4j Relationship Count Cache
------------------------------

[![Build Status](https://travis-ci.org/graphaware/neo4j-relcount.png)](https://travis-ci.org/graphaware/neo4j-relcount)

### Introduction

In some Neo4j applications, it is useful to know how many relationships of a given type, perhaps with different properties,
are present on a node. Naive on-demand relationship counting quickly becomes inefficient with large numbers of relationships
per node.

The aim of this GraphAware module is to provide an easy-to-use, transparent cache for relationship counts on nodes.

### Usage


Once set up (read below), it is very simple to use the API. To count `OUTGOING` relationships of type `FRIEND_OF` with property
 `level` equal to `2`, write:

```java
    Node node =... //find a node somewhere, perhaps in an index

    RelationshipCounter counter = new RelationshipCounterImpl(FRIEND_OF, OUTGOING).with("level", "2");

    int count = counter.count(node); //DONE!
```

#### Embedded Mode

When using Neo4j in _embedded_ mode, you need to register the component that does all the caching as a transaction event
handler on the database right after it has been created, like this:

```java
database = new GraphDatabaseFactory().newEmbeddedDatabase("/path/to/db");
database.registerTransactionEventHandler(new RelationshipCountTransactionEventHandlerFactory().create());
```

That's it!

#### Server Mode

Stay tuned, coming very soon!

### How does it work?

There is no magic. The transaction event handler intercepts all transactions before they are committed to the database
and analyzes them for any created, deleted, or modified relationships.

It caches the relationship counts as properties on each node, both for incoming and outgoing relationships. In order not
to pollute nodes with meaningless properties, a `RelationshipCountCompactor`, as the name suggests, compacts the cached
information.

Let's illustrate that on an example. Suppose that a node has no relationships to start with. When we create the first outgoing
relationship of type `FRIEND_OF` with properties `level` equal to `2` and `timestamp` equal to `1368206683579`, the following property
is automatically written to the node:

    _GA_REL_FRIEND_OF#OUTGOING#level#2#timestamp#1368206683579 = 1

At some point, after the node makes more friends, the situation will look something like this:

    _GA_REL_FRIEND_OF#OUTGOING#level#2#timestamp#1368206683579 = 1
    _GA_REL_FRIEND_OF#OUTGOING#level#2#timestamp#1368206668364 = 1
    _GA_REL_FRIEND_OF#OUTGOING#level#2#timestamp#1368206623759 = 1
    _GA_REL_FRIEND_OF#OUTGOING#level#2#timestamp#1368924528927 = 1
    _GA_REL_FRIEND_OF#OUTGOING#level#2#timestamp#1368092348239 = 1
    _GA_REL_FRIEND_OF#OUTGOING#level#2#timestamp#1368547772839 = 1
    _GA_REL_FRIEND_OF#OUTGOING#level#2#timestamp#1368542321123 = 1
    _GA_REL_FRIEND_OF#OUTGOING#level#2#timestamp#1368254232452 = 1
    _GA_REL_FRIEND_OF#OUTGOING#level#2#timestamp#1368546532344 = 1
    _GA_REL_FRIEND_OF#OUTGOING#level#2#timestamp#1363234542345 = 1
    _GA_REL_FRIEND_OF#OUTGOING#level#2#timestamp#1363234511676 = 1

At that point, the compactor looks at the situation and tries to generalize the cached relationships. One such generalization
might involve dropping the timestamp altogether, because it is unlikely we will ever want to count relationships with a specific
timestamp value for a single node (the probability of making 2 friendships within the same millisecond is close to 0).

So the compactor will try dropping the timestamp and generates a representation like this:

     _GA_REL_FRIEND_OF#OUTGOING#level#2

Then it looks at how many cached relationship counts match that representation. In our example, it is all of them (11). If
this number is above a *compaction threshold* (which is set to 10 by default), the compaction happens, resulting in

     _GA_REL_FRIEND_OF#OUTGOING#level#2 = 11

After that, 11 more friendships will get cached with the timestamp, before another compaction.

So that's how it works on a high level. Of course relationships with different levels of generality are supported
(for example, creating a `FRIEND_OF` relationship without a timestamp will work just fine). When issuing a query
 like this

 ```java
    RelationshipCounter counter = new RelationshipCounterImpl(FRIEND_OF, OUTGOING);
    int count = counter.count(node);
 ```

on a node with the following cache counts

      _GA_REL_FRIEND_OF#OUTGOING#level#2#timestamp#1368206683579 = 1
      _GA_REL_FRIEND_OF#OUTGOING#level#2 = 10
      _GA_REL_FRIEND_OF#OUTGOING#level#1 = 20
      _GA_REL_FRIEND_OF#OUTGOING = 5

the result will be... you guessed it... 36.

### Advanced Usage

There are a number of things that can be tweaked here. First of all, in order to change the compaction threshold,
just pass an integer parameter into the `RelationshipCountTransactionEventHandlerFactory` `create` method.

```java
int threshold = 20;
database = new GraphDatabaseFactory().newEmbeddedDatabase("/path/to/db");
database.registerTransactionEventHandler(new RelationshipCountTransactionEventHandlerFactory().create(threshold));
```

To exclude certain relationships from the count caching process altogether, create a strategy that implements the
`RelationshipInclusionStrategy` interface and implement the following method:

```java
    /**
     * Should this relationship be included for the purposes of relationship count caching.
     *
     * @param relationship to check.
     * @return true iff the relationship should be included.
     */
    boolean include(Relationship relationship);
```

Then just pass your new strategy to the `RelationshipCountTransactionEventHandlerFactory` `create` method.

Excluding specific properties on relationships should rarely be necessary. However, should you desire to do so,
or even create your own "derived" properties on relationships to be cached, implement the `PropertyExtractionStrategy`
and pass it on to the factory as well. You will need to implement a single method:

```java
    /**
     * Extract properties from a relationship for the purposes of caching the relationship's count on a node (a.k.a. "this node").
     *
     * @param properties attached to the relationship.
     * @param otherNode  the other node participating in the relationship. By "other", we mean NOT the node on which
     *                   the relationship counts for this relationship are being updated as a part of this call.
     * @return extracted properties for count caching.
     */
    Map<String, String> extractProperties(Map<String, String> properties, Node otherNode);
```

As you can see, this gives you access to the "other" node participating in the relationship. This gives you an opportunity
to implement requirements like "would like to count outgoing relationships based on the end node's type".

### Known Issues

* Check whether a relationship is GraphAware internal should be removed from inclusion strategy
* Create a method that rebuilds cached counts for the entire database
* Test that relationship inclusion and property extraction strategies are properly honored
* Measure and improve performance (always! :-))

### License

Copyright (c) 2013 GraphAware

GraphAware is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License
as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
You should have received a copy of the GNU General Public License along with this program.  If not, see <http://www.gnu.org/licenses/>.