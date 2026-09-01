package com.rideshare.locationservice.service;

import com.rideshare.locationservice.dto.DriverLocationRequest;
import com.rideshare.locationservice.dto.NearbyDriverResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.geo.*;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class LocationService {

    private final RedisTemplate<String, String> redisTemplate;

    // Redis key for all driver location
    private static final String DRIVERS_GEO_KEY = "drivers:locations";

    // Update driver location in Redis
    // Called every 3 seconds by driver's phone
    // Maps to Redis GEOADD command
    public void updateDriverLocation(DriverLocationRequest request) {
        log.info("Updating location for driver: {}", request.getDriverId());

        // IMPORTANT: longitude FIRST, latitude SECOND - GeoSpecial Standard
        Point driverPoint = new Point(
                request.getLongitude(),
                request.getLatitude()
        );

        // opsForGeo() -> This method gives access to all Redis GEO special commands in java
        // here we update the driver location
        redisTemplate.opsForGeo().add(
                DRIVERS_GEO_KEY,
                driverPoint,
                request.getDriverId()
        );

        log.info("Location updated for driver: {}", request.getDriverId());
    }

    // Find nearby drivers within given radius
    // Called by Matching Service on ride request
    // Map to Redis GEORADIUS command
    public List<NearbyDriverResponse> findNearByDrivers(double latitude, double longitude, double radiusInKm) {
        log.info("Finding drivers near lat: {} long: {} within {}km",
                latitude,longitude, radiusInKm);

        // Create a search area for example 5km -> radiusInKm = 5
        Circle searchArea = new Circle(
                new Point(longitude, latitude),
                new Distance(radiusInKm, Metrics.KILOMETERS)
        );

        // Now get the drivers in the search area
        GeoResults<RedisGeoCommands.GeoLocation<String>> results =
                redisTemplate.opsForGeo().radius(
                        DRIVERS_GEO_KEY,
                        searchArea,
                        RedisGeoCommands.GeoRadiusCommandArgs.newGeoRadiusArgs()
                                .includeCoordinates()
                                .includeDistance()
                                .sortAscending()
                                .limit(10)
                );

        List<NearbyDriverResponse> nearbyDrivers = new ArrayList<>();

        if(results != null) {
            // Here getting list of drivers and applying foreach loop on this list
            results.getContent().forEach(result -> {
                // For one driver
                // First get driver location details
                RedisGeoCommands.GeoLocation<String> location = result.getContent();
                // Then adding fetched drivers location details to the list of nearby drivers
                nearbyDrivers.add(new NearbyDriverResponse(
                        location.getName(),
                        location.getPoint().getY(),
                        location.getPoint().getX(),
                        result.getDistance().getValue()
                ));
            });
        }
        log.info("Found {} drivers nearby", nearbyDrivers.size());

        return nearbyDrivers;
    }

    // Remove drivers when they go offline
    // Maps to Redis ZREM command
    public void removeDriver(String driverId) {
        log.info("Removing driver: {}", driverId);
        redisTemplate.opsForGeo().remove(DRIVERS_GEO_KEY, driverId);
    }
}
