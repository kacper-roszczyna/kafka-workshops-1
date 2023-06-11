package pl.roszczyna.movies.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.avro.reflect.Nullable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Movie {

    public String title;

    @Nullable
    public Integer rating;

}
