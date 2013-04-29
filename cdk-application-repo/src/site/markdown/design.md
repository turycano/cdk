# CDK Application Module

This is the design draft for the Application Repository module.

## Status

This draft is under development. It:

* may not be technically accurate.
* may not be internally consistent.
* may not be ultimately accepted or implemented.
* is open for input.

No guarantees are provided as to the current state of the associated prototype
code. Users should __not__ depend on this code.

## Overview

* Problem statement
* How we're going to fix it (benefits)
* What we're not doing (limitations)

It's very common that users develop tools and processes that operate on data in
the Hadoop ecosystem. These applications are usually considered "back end" or
system applications. They run either from the command line, or from other back
end systems such as schedulers. To date, developers haven't had a consistent
way to deploy, share, track, manage, and execute these processes. This draft
proposes a standard data processing application (or "app") packaging format and
a set of APIs that facilitate the operations mentioned earlier. Additionally,
an _application repository_ concept is provided, which acts as a collection of
deployed applications and associated artifacts.

The working title of the CDK module containing the definition of the application
packaging standard, the APIs that operate on those applications, the
execution infrastructure, and tooling integration is called the CDK Application
module. This draft will use the capitalized word _Applications_ or _CDK
Applications_ when referring to the format standardized by this module.

## Applications

* Application types
* Packaging
* Extensibility - Defining new application types

The term _application_ can mean different things to different people. This draft
is exclusively focused on the back end data processing applications described
in the overview section.

Examples of data processing applications that are considered in scope:

* Java MapReduce jobs
* Hive queries
* Impala queries
* Sqoop import / export jobs
* File import / export jobs
* Oozie workflows

It's important to point that out we do __not__ believe, for example, ad hoc
Impala, Hive, or Pig queries should be deployed as Applications. The existing
interactive shells and BI workbenches are far more appropriate for "single shot"
operations such as these. If, instead, a critical Pig script, for example, is
developed that should be shared between users or systems of a cluster, it is
probably a candidate for inclusion in the earlier definition of an Application.
What is or isn't an Application is entirely at the user's descretion. If it is
deemed important enough to share, track over time, collect metrics for, or
execute in a reliable and reproduceable manner, such an query or process should
be considered an Application.

### Format

CDK Applications are packaged as either _self contained jar files_ - a jar
file containing the application and any dependencies - or non-self contained jar
file, where dependencies are stated in the jar's MANIFEST.MF file and are also
deployed within the same application repository (described later).

Rather than invent a new format, we simply use the [OSGi][osgi]
[bundle format][osgi-bf].

[osgi]: http://www.osgi.org/Technology/WhatIsOSGi
[osgi-bf]: http://en.wikipedia.org/wiki/OSGi#Bundles

Historically, OSGi has been seen as cumbersome and painful. However, in this
context, we believe our use can be heavily restricted to the simplest case. In
fact, the sole reason OSGi was selected was to facilitate simple dynamic loading
and execution of jar files without the need to spawn a separate process (in
addition to Tom's sage advice of "don't invent a new packaging format"). Beyond
that, we never run OSGi bundles in a persistent container, force the use of the
service model, or deal with other complex topics. We anticipate most users will
build self contained jar files - the Java equivalent of static linking - since
this is already familiar to users who produce war files today, in addition to
the simplicity of dependency resolution. Under this model, jars need not even
be OSGi bundles since no package exports or imports are required, and jars can
easily be wrapped with minimal OSGi headers. This is the format we'll focus on,
initially, with non-self contained jars being supported in the future.

Contained within the MANFIEST.MF, additional headers can be used to indicate
the type of application. The following properties are supported:

CDK-AppType

The type of the application. The value of this property is a registered
application type that determines the executor (i.e. launcher) implementation.
Users can extend the list of supported application types. In fact, any string
here must only have a registered implementation class in the ApplicationExecutor
APIs (described later).

Additionally, some of the OSGi properties are reused.

Bundle-SymbolicName

This OSGi property is also used to indicate the canonical name of the
application. It is written in reverse-domain format, the same as Java package
names. The symbol name can be formed from Maven artifacts, for example, by
appending the artifactId to the groupId.

Bundle-Version

Another OSGi property, Bundle-Version is used to indicate the bundle version.

## Application Repositories

* Application Lifecycle
* Integration with tooling

## Execution

* Execution lifecycle
* Environment
* Extensibility - Adding new executors
