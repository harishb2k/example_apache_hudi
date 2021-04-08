package com.devlibx.data.source;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Ride {
    private String id;
    private String pickup;
    private String drop;
    private Double price;
    private String time;
}
