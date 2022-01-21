package utils;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@AllArgsConstructor
@Data
public class PutData implements Serializable {
    private Integer dataSize;
    private String dataAddress;
}
