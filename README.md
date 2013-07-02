Neo4j Relationship Count Cache
------------------------------

[![Build Status](https://travis-ci.org/graphaware/neo4j-relcount.png)](https://travis-ci.org/graphaware/neo4j-relcount)

### Introduction

In some Neo4j applications, it is useful to know how many relationships of a given type, perhaps with different properties,
are present on a node. Naive on-demand relationship counting quickly becomes inefficient with large numbers of relationships
per node.

The aim of this GraphAware module is to provide an easy-to-use, transparent cache for relationship counts on nodes.

### Download

Releases are synced to Maven Central repository. In order to use the latest release, include the following snippet
in your pom.xml:

    <dependencies>
        ...
        <dependency>
            <groupId>com.graphaware</groupId>
            <artifactId>neo4j-relcount</artifactId>
            <version>1.1</version>
        </dependency>
        ...
     </dependencies>

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

    _GA_REL_FRIEND_OF#OUTGOING#_LITERAL_#true#level#2#timestamp#1368206683579 = 1

Let's break it down:
* `_GA_REL_` is a prefix for GraphAware internal metadata.
* `FRIEND_OF` is the relationship type
* `#` is a separator GraphAware uses internally.
* `_LITERAL_#true` indicates this is literally what has been created, rather than a compacted representation (more about compaction later).
* `level` is the key of the first property
* `2` is the value of the first property (level)
* `timestamp` is the key of the second property
* `1368206683579` is the value of the second property (timestamp)
* `1` is the cached number of relationships matching this representation (stored as a value of a property)

*NOTE:* None of the application level nodes or relationships should have names, types, labels, property keys or values containing the following Strings:
* `_GA_REL_`
* `_LITERAL_`
* `#`
That includes user input written into properties of nodes and relationship. Please check for this in your application and
encode it somehow.

Right, at some point, after the node makes more friends, the situation will look something like this:

    _GA_REL_FRIEND_OF#OUTGOING#_LITERAL_#true#level#2#timestamp#1368206683579 = 1
    _GA_REL_FRIEND_OF#OUTGOING#_LITERAL_#true#level#1#timestamp#1368206668364 = 1
    _GA_REL_FRIEND_OF#OUTGOING#_LITERAL_#true#level#2#timestamp#1368206623759 = 1
    _GA_REL_FRIEND_OF#OUTGOING#_LITERAL_#true#level#2#timestamp#1368924528927 = 1
    _GA_REL_FRIEND_OF#OUTGOING#_LITERAL_#true#level#0#timestamp#1368092348239 = 1
    _GA_REL_FRIEND_OF#OUTGOING#_LITERAL_#true#level#2#timestamp#1368547772839 = 1
    _GA_REL_FRIEND_OF#OUTGOING#_LITERAL_#true#level#1#timestamp#1368542321123 = 1
    _GA_REL_FRIEND_OF#OUTGOING#_LITERAL_#true#level#2#timestamp#1368254232452 = 1
    _GA_REL_FRIEND_OF#OUTGOING#_LITERAL_#true#level#1#timestamp#1368546532344 = 1
    _GA_REL_FRIEND_OF#OUTGOING#_LITERAL_#true#level#0#timestamp#1363234542345 = 1

At that point, the compactor looks at the situation finds out there are too many cached relationship counts. More specifically,
there is a threshold called the _compaction threshold_ which by default is set to 20. We will illustrate with 10.

The compactor thus tries to generalize the cached relationships. One such generalization
might involve dropping the timestamp, generating representations like this:

    _GA_REL_FRIEND_OF#OUTGOING#level#0
    _GA_REL_FRIEND_OF#OUTGOING#level#1
    _GA_REL_FRIEND_OF#OUTGOING#level#2

Then it compacts the cached relationship counts that match these representations. In our example, it results in this:

     _GA_REL_FRIEND_OF#OUTGOING#level#0 = 2
     _GA_REL_FRIEND_OF#OUTGOING#level#1 = 3
     _GA_REL_FRIEND_OF#OUTGOING#level#2 = 5

After that, timestamp will always be ignored for these relationships, so if the next created relationships is

    _GA_REL_FRIEND_OF#OUTGOING#_LITERAL_level#0#timestamp#1363266542345

it will result in

    _GA_REL_FRIEND_OF#OUTGOING#level#0 = 3
    _GA_REL_FRIEND_OF#OUTGOING#level#1 = 3
    _GA_REL_FRIEND_OF#OUTGOING#level#2 = 5

So that's how it works on a high level. Of course relationships with different levels of generality are supported
(for example, creating a `FRIEND_OF` relationship without a level will work just fine). When issuing a query
 like this

 ```java
    RelationshipCounter counter = new RelationshipCounterImpl(FRIEND_OF, OUTGOING);
    int count = counter.count(node);
 ```

on a node with the following cache counts

      _GA_REL_FRIEND_OF#OUTGOING#_LITERAL_#true#level#3#timestamp#1368206683579 = 1
      _GA_REL_FRIEND_OF#OUTGOING#level#2 = 10
      _GA_REL_FRIEND_OF#OUTGOING#level#1 = 20
      _GA_REL_FRIEND_OF#OUTGOING#_LITERAL_#true = 5

the result will be... you guessed it... 36.

### Advanced Usage

There are a number of things that can be tweaked here. First of all, in order to change the compaction threshold,
just pass an integer parameter into the `RelationshipCountTransactionEventHandlerFactory` `create` method.

```java
int threshold = 30;
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
     * @param properties attached to the relationship. Don't modify these (you'll get an exception), create a new map instead.
     * @param otherNode  the other node participating in the relationship. By "other", we mean NOT the node on which
     *                   the relationship counts for this relationship are being updated as a part of this call.
     * @return extracted properties for count caching.
     */
    Map<String, String> extractProperties(Map<String, String> properties, Node otherNode);
```

As you can see, this gives you access to the "other" node participating in the relationship, which gives you an opportunity
to implement requirements like "would like to count outgoing relationships based on the end node's type".

### TODO

* Create a method that rebuilds cached counts for the entire database
* Do we need to explicitly lock nodes?
* Measure and improve performance (always! :-))

### License

Copyright (c) 2013 GraphAware

GraphAware is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License
as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
You should have received a copy of the GNU General Public License along with this program.
If not, see <http://www.gnu.org/licenses/>.