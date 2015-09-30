DemocracIT Project
================
*DemocracIT Scheduler*

-------------------------------------------------------------------------------------------
Structure
-------------------------------------------------------------------------------------------

This module is responsible for controlling all the backend processes for the DemocracIT project.
> **Contains:**
> - Scheduler

The scheduler is controlled by scheduler.py. Inject all the classes needed to load via a yaml configuration and execute it. It currently executes crawler, indexer, wordcloud extractor, and all three implementations of FekAnnotator.
