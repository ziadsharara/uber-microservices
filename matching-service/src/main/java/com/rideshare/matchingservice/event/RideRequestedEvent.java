package com.rideshare.matchingservice.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

// Event consumed from Kafka topic: ride.requested
// Published by Ride Service when a rider request a ride
@Data
@AllArgsConstructor
@NoArgsConstructor
public class RideRequestedEvent {

    private String rideId;
    private String riderId;

    // PICKUP
    private double pickupLatitude;
    private double pickupLongitude;
    private String pickupAddress;

    // DROP
    private double dropLatitude;
    private double dropLongitude;
    private String dropAddress;

}
