# siddhi-execution-approximate
The **siddhi-execution-approximate** is an extension to <a target="_blank" href="https://wso2.github
.io/siddhi">Siddhi</a>  that performs approximate computing on event streams.

Find some useful links below:
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-execution-approximate">Source code</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-execution-approximate/releases">Releases</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-execution-approximate/issues">Issue tracker</a>

## Latest API Docs

Latest API Docs is <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-approximate/api/1.0.2-SNAPSHOT">1.0.2-SNAPSHOT</a>.

## How to use 

**Using the extension in <a target="_blank" href="https://github.com/wso2/product-sp">WSO2 Stream Processor</a>**

* You can use this extension in the latest <a target="_blank" href="https://github.com/wso2/product-sp/releases">WSO2 Stream Processor</a> that is a part of <a target="_blank" href="http://wso2.com/analytics?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">WSO2 Analytics</a> offering, with editor, debugger and simulation support. 

* This extension is shipped by default with WSO2 Stream Processor, if you wish to use an alternative version of this extension you can replace the component <a target="_blank" href="https://https://github.com/wso2-extensions/siddhi-execution-approximate/releases">jar</a> that can be found in the `<STREAM_PROCESSOR_HOME>/lib` directory.

**Using the extension as a <a target="_blank" href="https://wso2.github.io/siddhi/documentation/running-as-a-java-library">java library</a>**

* This extension can be added as a maven dependency along with other Siddhi dependencies to your project.

```
     <dependency>
        <groupId>org.wso2.extension.siddhi.execution.approximate</groupId>
        <artifactId>siddhi-execution-approximate</artifactId>
        <version>x.x.x</version>
     </dependency>
```

## Features

* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-approximate/api/1.0.2-SNAPSHOT/#distinctcountever-stream-processor">distinctCountEver</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#stream-processors">Stream Processor</a>)*<br><div style="padding-left: 1em;"><p>Performs HyperLogLog algorithm on a streaming data set based on a specific relative error and a confidence value to calculate the number of distinct events.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-approximate/api/1.0.2-SNAPSHOT/#count-stream-processor">count</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#stream-processors">Stream Processor</a>)*<br><div style="padding-left: 1em;"><p>Performs Count-min-sketch algorithm on a window of streaming data set based on a specific relative error and  a confidence value to calculate the approximate count(frequency) of events.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-approximate/api/1.0.2-SNAPSHOT/#distinctcount-stream-processor">distinctCount</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#stream-processors">Stream Processor</a>)*<br><div style="padding-left: 1em;"><p>Performs HyperLogLog algorithm on a window of streaming data set based on a specific relative error and a confidence value to calculate the number of distinct events.</p></div>

## How to Contribute
 
  * Please report issues at <a target="_blank" href="https://github.com/wso2-extensions/siddhi-execution-approximate/issues">GitHub Issue Tracker</a>.
  
  * Send your contributions as pull requests to <a target="_blank" href="https://github.com/wso2-extensions/siddhi-execution-approximate/tree/master">master branch</a>. 
 
## Contact us 

 * Post your questions with the <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">"Siddhi"</a> tag in <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">Stackoverflow</a>. 
 
 * Siddhi developers can be contacted via the mailing lists:
 
    Developers List   : [dev@wso2.org](mailto:dev@wso2.org)
    
    Architecture List : [architecture@wso2.org](mailto:architecture@wso2.org)
 
## Support 

* We are committed to ensuring support for this extension in production. Our unique approach ensures that all support leverages our open development methodology and is provided by the very same engineers who build the technology. 

* For more details and to take advantage of this unique opportunity contact us via <a target="_blank" href="http://wso2.com/support?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">http://wso2.com/support/</a>. 
