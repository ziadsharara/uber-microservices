package com.rideshare.rideservice.dto;

import com.rideshare.rideservice.model.RideStatus;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class RideResponse {

    private String id;

    private String rideId;

    private String driverId;

    private double pickupLatitude;
    private double pickupLongitude;
    private String pickupAddress;

    private double dropLatitude;
    private double dropLongitude;
    private String dropAddress;

    private RideStatus status;

    private double estimatedFare;
    private double actualFare;

    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;

    private LocalDateTime startAt;
    private LocalDateTime completedAt;
}
