# Packr - a Java BagIt Library #

This project contains a lightweight java library to support creation and consumption of BagIt-packaged content, as specified
by the BagIt Spec (IETF RFC 8493 version 1.0). It requires a Java 17 or better to run, has a single dependency on the Apache
commons compression library for support of tarred Gzip archive format (".tgz"), and is Apache 2 licensed. Build with Gradle.

[![Build Status](https://github.com/richardrodgers/packr/actions/workflows/gradle.yml/badge.svg?branch=main)](https://github.com/richardrodgers/packr/actions/workflows/gradle.yml)
[![javadoc](https://javadoc.io/badge2/edu.mit.lib/bagit/javadoc.svg)](https://javadoc.io/doc/edu.mit.lib/bagit)

## Use Cases ##

The library attempts to simplify a few of the most common use cases/patterns involving bag packages.
The first (the _producer_ pattern) is where content is assembled and placed into a bag, and the bag is then serialized
for transport/hand-off to another component or system. The goal here is to ensure that the constructed bag is correct.
Two helper classes - _BagBuilder_  and _Serde_ - are used to orchestrate this assembly.

    Sequence: new BagBuilder -> add content -> add more content -> Serde.serialize

The second (the _consumer_ pattern) is where a bag serialization (or a loose directory) is given and must
be interpreted and validated for use. Here the helper class - _Serde_ - is used to deserialize.

    Sequence: deserialize and convert to Bag -> process contents
    
If you have more complex needs
in java, (e.g. support for multiple spec versions), you may wish to consider the [Library of Congress Java Library](https://github.com/LibraryOfCongress/bagit-java).

## Creating Bags (producer pattern) ##

A very simple 'fluent' builder interface is used to create bags, where content is added utilizing an object called
a _BagBuilder_. For example, to create a bag with a few files (here the java.nio.file.Path instances 'file1', 'file2'):

    Bag bag = new BagBuilder().payload(file1).payload(file2).build();

Metadata (in tag files, default: _bag-info.txt_) can be added in the same fluent manner:

    builder = builder.metadata("Contact-Name", "Joe Bloggs").metadata("Contact-Email", "bloggsj@gmail.com");

Since bags are often used to _transmit_ packaged content, we would typically next obtain a serialization of the bag:

    InputStream bagStream = Serde.toStream(bag);

This would be a very natural way to export bagged content to a network service. A few defaults are at work in
this invocation, e.g. the _toStream()_ method with no extra arguments uses the default package serialization, which is a zip
archive. To convert the same bag to use a compressed tar format:

    InputStream bagStream = Serde.toStream(bag, "tgz");

We don't always want bag I/O streams - suppose we wish to obtain a bag archive file package instead:

    Bag bag = new BagBuilder().payload(file1).metadata("External-Identifier", "mit.edu.0001").build();
    Path bagPackage = Serde.toPackage(bag);

Another default in use so far has been that the Builder constructor (_new BagBuilder()_) is not given a directory path
for the bag. In this case, a temporary directory for the bag is created. This has several implications, depending on how the builder is later used.  If a stream is requested (as in the first example above), the temporary bag will be automatically deleted as soon as the reader stream is closed. This is very convenient when used to transport bags to a network service - no clean-up is required:

    InputStream tempStream = Serde.toStream(new BagBuilder().payload(myPayload).build());
    // consume stream
    tempStream.close();

If a package or directory is serialized (as opposed to a stream), the bag directory or file returned will be
deleted upon JVM exit only, which means that bag storage management could become an issue for a large number of
files and/or a long-running JVM. Thus good practice would be to either: have the client copy the bag package/directory
to a new location for long term preservation if desired, or timely client deletion of the package/directory so storage
is not taxed.

On the other hand, if a directory is passed to the BagBuilder in the constructor, it will _not_ be considered temporary
and thus not be removed on stream close or JVM exit.

For example, can choose to access the bag contents as an (unserialized) directory in the file system comprising the bag.
In this case we need to indicate where we want to put the bag when we construct it:

    Bag bag = new BagBuilder(myDir).payload(file1).payloadRef("file2", file2, http://www.foo.com/data.0002").build();
    // use 'myDir directly or via Bag reference

## Reading Bags (consumer pattern) ##

The reverse situation occurs when we wish to read or consume a bag. Here we are given a specific representation of
a purported bag, (viz. directory, archive, I/O stream), and need to interpret it (and possibly validate it). The helper class in this case is 'Serde', which is used to produce Bag instances from package serializations (including the _null_ 
serialization case of a filesystem loose directory). Thus:

    Bag bag = Serde.fromPackage(myZipFile);
    Path myBagFile = bag.payloadFile("firstSet/firstFile");

Or the bag contents may be obtained from a network stream:

    String bagId = Serde.fromStream(inputStream, "zip").metadata("External-Identifier");

For all the API details consult the [Javadoc](https://javadoc.io/doc/edu.mit.lib/bagit)

## Portability ##

Bags are intended to be portable data containers, in the sense that one should be able to write them on one operating system,
and read them on another. The spec contemplates this in specific ways, e.g. by allowing text files such as
'bag-info.txt' legally to have _either_ Unix-style line termination, or Windows-style. Tools operating on bags ought
to expect and tolerate this diversity, but do not always. The library provides some assistance here by allowing the user
to specify a preference when creating bags. Thus, if the context of use (lifecycle) for a set of bags is known to be in
a Windows environment, the library can be instructed to use Windows line termination for the generated text files in bags,
even if the bags are being generated on a Unix system. By default, the library will use the termination of the
operating system it is running on ('CR/LF' on Windows, '\n' on Unix and MacOS), but this can be overridden.
See the [Javadoc](https://javadoc.io/doc/edu.mit.lib/bagit) for details.

## Archive formats ##

Bags are commonly serialized to standard archive formats such as ZIP. The library supports three (two compressed) archive formats:
'tar', 'zip' and 'tgz' (GZip'ed tar) and a variant in each of these. If the variant is used, the library suppresses the file
creation/modification time attributes, in order that checksums of archives produced at different times
may accurately reflect only bag contents. That is, the checksum of a zipped bag (with no timestamp variant) is
time-of-archiving and filesystem-time-invariant, but content-sensitive. The variant is requested with an API call.

## Extras and Advanced Features ##

The library supports a few features not required by the BagIt spec. One is basic automatic
metadata generation. There are a small number of reserved properties typically recorded in _bag-info.txt_
that can easily be determined by the library. These values are automatically populated in _bag-info.txt_ by default.
The 'auto-fill' properties are: Bagging-Date, Bag-Size, Payload-Oxum, and one non-reserved property 'Bag-Software-Agent'.
If automatic generation is not desired, an API call disables it.

Another extra is _opaque_ bags. Bags created by builders or deserialized from packages/streams are immutable,
meaning they cannot be altered via the API. But we typically _can_ gain access to the backing bag storage,
which we can of course then change at will. However, if a bag is created as _opaque_ (a method on the builder or deserialzer),
all method calls that expose the underlying storage will throw IllegalAccess exceptions. So, for example, we would
be _unable_ to obtain a File reference to a payload file, but _could_ get an I/O stream to the same content.
In other words, the content can be accessed, but the underlying representation cannot be altered, and
to this degree the bag contents are _tamper-proof_.

### Download ###

The distribution jars are kept at Maven Central, so make sure that repository is declared.
Then (NB: using the most current version), for Gradle:

    implementation 'org.modrepo:packr:1.2'

or Maven:

    <dependency>
      <groupId>org.modrepo</groupId>
      <artifactId>packr</artifactId>
      <version>1.2</version>
    </dependency>

in a standard pom.xml dependencies block.
