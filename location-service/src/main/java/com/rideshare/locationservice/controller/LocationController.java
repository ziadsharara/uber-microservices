package com.rideshare.locationservice.controller;

import com.rideshare.locationservice.dto.DriverLocationRequest;
import com.rideshare.locationservice.dto.NearbyDriverResponse;
import com.rideshare.locationservice.service.LocationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/locations")
@Slf4j
@RequiredArgsConstructor
public class LocationController {

    private final LocationService locationService;

    // Driver phone calls this every 3 seconds
    @PostMapping("/drivers/update")
    public ResponseEntity<String> updateDriverLocation(@RequestBody DriverLocationRequest request) {

        locationService.updateDriverLocation(request);
        return ResponseEntity.ok("Driver Location Updated");
    }

    // Matching service calls this when ride is requested
    @GetMapping("/drivers/nearby")
    public ResponseEntity<List<NearbyDriverResponse>> getNearByDrivers(@RequestParam double latitude,
                                                                       @RequestParam double longitude,
                                                                       @RequestParam(defaultValue = "5.0") double radius) {

        return ResponseEntity.ok(locationService.findNearByDrivers(latitude, longitude, radius));
    }

    // Called when driver goes offline
    @DeleteMapping("/drivers/{driverId}")
    public ResponseEntity<String> removeDriver(@PathVariable String driverId) {
        locationService.removeDriver(driverId);
        return ResponseEntity.ok("Driver removed successfully");
    }
}
