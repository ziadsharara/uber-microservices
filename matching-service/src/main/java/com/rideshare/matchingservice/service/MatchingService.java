package com.rideshare.matchingservice.service;

import com.rideshare.matchingservice.client.LocationServiceClient;
import com.rideshare.matchingservice.dto.NearByDriverResponse;
import com.rideshare.matchingservice.event.RideMatchedEvent;
import com.rideshare.matchingservice.event.RideRequestedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

@Service
@Slf4j
@RequiredArgsConstructor
public class MatchingService {

    private final LocationServiceClient locationServiceClient;
    private final KafkaTemplate<String, RideMatchedEvent> kafkaTemplate;

    private static final String RIDE_MATCHED_TOPIC = "ride.matched";
    private static final double DEFAULT_SEARCH_RADIUS_KM = 5.0;

    /**
     * Main matching algorithm
     * Called when RideRequestEvent is consumed from Kafka
     *
     * STEPS:
     * 1. Ask Location Service for nearby drivers
     * 2. Score each driver and pick the best one
     * 3. Publish RideMatchedEvent to Kafka
     * @param event
     * */
    public void matchDriverForRide(RideRequestedEvent event) {

        // Step 1
        List<NearByDriverResponse> nearbyDrivers = locationServiceClient.getNearByDrivers(
                event.getPickupLatitude(),
                event.getPickupLongitude(),
                DEFAULT_SEARCH_RADIUS_KM
        );

        if (nearbyDrivers.isEmpty()) {
            log.warn("No drivers found near ride");
            return;
        }


        // Step 2
        Optional<NearByDriverResponse> bestDriver = findBestDriver(nearbyDrivers);

        if (bestDriver.isEmpty()) {
            log.warn("Couldn't find suitable driver for ride");
            return;
        }

        NearByDriverResponse assignedDriver = bestDriver.get();


        // Step 3
        RideMatchedEvent matchedEvent = new RideMatchedEvent(
                event.getRideId(),
                event.getRiderId(),
                assignedDriver.getDriverId(),
                assignedDriver.getLatitude(),
                assignedDriver.getLongitude(),
                assignedDriver.getDistanceInKm()
        );

        kafkaTemplate.send(RIDE_MATCHED_TOPIC, event.getRideId(), matchedEvent);
        log.info("RideMatchedEvent published!");
    }

    /**
     * Driver Scoring algorithm
     *
     * Distance: 70%
     * Rating: 30%
     *
     * Score = (1 / distance) * distanceWeight + rating * ratingWeight
     * @param drivers
     * @return
     * */
    private Optional<NearByDriverResponse> findBestDriver(List<NearByDriverResponse> drivers) {

        double distanceWeight = 0.7;
        double ratingWeight = 0.3;

        return drivers.stream()
                .max(Comparator.comparingDouble(driver -> {
                    // Distance score: closer = higher score
                    // Add 0.1 to avoid division by zero
                    double distanceScore = 1.0 / (driver.getDistanceInKm() + 0.1);

                    // Simulated rating between 4.0 and 5.0
                    // In production: fetch from driver service
                    double simulatedRating = 4.0 + Math.random();

                    // Final weighted score
                    return (distanceScore * distanceWeight)
                            + (simulatedRating * ratingWeight);
                }));
    }
}
