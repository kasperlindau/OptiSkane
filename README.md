# OptiSkane
Python backend for finding the best public transit journeys available at any given time within Skåne. 

Also configurable for other transit regions supported by TrafikLabs API.

Corresponding app: https://github.com/kasperlindau/OptiSkaneApp

# About
So I have never been really pleased with Skånetrafiken's current app. It often only suggests a few suboptimal journeys and is not very customizable. Another problem is that they use target-stops as destination, which limits the results even more. The reason for this is that they probably are aiming to distribute their travelers as well as possible on all of their available vehicles. Another reason may be that they need to adapt their service so it satisfies the average traveler. They also have to take computational power into account, as they deal with a lot of users each day, especially during rush hours.

Modern problem requires modern solutions!! The only logical solution was to build an app myself.

# Specifications
Firstly, I implemented the A* search algorithm. It produced good results but was waaaaaaay too slow.  
Then I implemented a variant of the CSA algorithm. This was much faster but I had hard times customizing it to fit my needs.  
So I ended up using Microsoft's RAPTOR algorithm, based on this [paper](https://www.microsoft.com/en-us/research/wp-content/uploads/2012/01/raptor_alenex.pdf). The only difference is that I do not use target-stops for pruning.

Foot-paths are currently estimated by:
* Calculating haversine distance between ALL stop pairs
* Filtering pairs where the distance is under a customizable MAX_WALK_RADIUS (currently set to 1km)
* Converting the distances to estimated time by WALK_SPEED (currently set to 5km/h) and penalizing it with a factor of 2

A more accurate way would be to use real GPS routes instead of just the penalized linear distance.  
The benefit of using a quite large MAX_WALK_RADIUS is that it can find faster routes.  
The downside is that it is much more computationally expensive.

The only inputs needed to perform a search are: origin coordinates, destination coordinates, and departure time. 
* I start by finding starting-stops within the MAX_WALK_RADIUS from the origin coordinates
* I then use the RAPTOR algorithm to find the most optimal journeys given the departure time
* I then, for each ending-stop that is within the MAX_WALK_RADIUS from the destination coordinates, extract the journey
* Lastly, I filter all the returned journeys

For providing an API, I use [FastiAPI](https://github.com/tiangolo/fastapi).
For all data, both static and real-time, I use [TrafikLab]("https://www.trafiklab.se/").

# Performance

# Further improvements
* Foot-paths could be GPS distance for more accurate results
* Python could be switched to a more performant language to improve performance
* Parallelization could be used when searching for journeys to improve performance



