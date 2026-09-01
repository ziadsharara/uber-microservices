package com.rideshare.rideservice.event;

// Event published to Kafka when ride is requested
// MMatching service will consume this event
// TPOIC: ride.requested

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class RideRequestEvent {

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
