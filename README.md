Remote sensing products of atmospheric clouds and aerosols from MODIS are one of the most widely used data sets for study Earth-Atmosphere systems. Support is requested to refactor the current MODIS atmosphere Level-3 product aggregation and distribution system to drastically reduce data generation and processing time, and to facilitate user-defined customization, using the framework of Advanced Data Analytics Platform (ADAPT) and Data Analytics and Storage System (DASS) provided by the NASA Center for Climate Simulation (NCCS). We believe the Climate Analytics-as-a-Service (CAaaS) framework provided by ADAPT and DASS is well suited for the proposed project. Specifically, the major objectives are to:
1.    From a system developer perspective, revolutionize the efficiency for generating the current MODIS Level-3 products. The objective can be achieved through scalable, generation of gridded (Level-3) MODIS products from pixel-level (Level-2) products based on the Analytic Service design of CAaaS and the Big Data analytics capability of ADAPT/DASS.
2.    From a system customer perspective, enable flexible, on-demand MODIS data access and analytics capability, and thereby shift the Level-3 data paradigm from static product to science-oriented service. The objective can be achieved through customizable generation and science-oriented data distribution of Level-3 MODIS data using the Analytic and Persistent Service design of CAaaS and the Big Data analytics and storage capability of ADAPT/DASS.
The following engineering and research activities are proposed to be undertaken to achieve the above objectives:
1.    Refactoring existing MODIS product storage and distribution
a.    Storing MODIS Level-2 and Level-3 product on DASS/ADAPT.
b.    Supporting customized data discovery/access services based on user defined time, geographical location and variables.
2.    Refactoring existing MODIS product generation and analytics
a.    Parallelizing existing MODIS Level-2 to Level-3 aggregation programs by following the MapReduce programing model and the microservice model.
b.    Supporting user-defined Level-2 data aggregation by integrating flexible data access and analytics.
3.    Evaluation
a.    Execution Performance Evaluation.
b.    Usability Evaluation.
c.     Extensibility Evaluation.
The deliverables of the proposed project include: 1) open source software implemented based on the activities; 2) openly available documents on software design, deployment, testing; 3) openly available evaluation reports on performance, usability and extensibility of the developed and deployed system on ADAPT/DASS.
