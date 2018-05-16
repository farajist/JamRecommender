package org.jam.recommendation;


import java.util.List;

/** represents a single field, probably done for the sake of allowing
 * exclusion and useen recommendation through the control of the
 * bias parameter
 *
 * @param name name of metadata field
 * @param values fields can have multiple values like tags of a signle
 * values or when using hierarchical taxonomies
 * @param bias positive value is a boost, negative is a filter
 */

public class Field {

    private final String name;
    private final List<String> values;
    private final Float bias;

    public Field(String name, List<String> values, Float bias) {
        this.name = name;
        this.values = values;
        this.bias = bias;
    }

    public String getName() {
        return name;
    }

    public List<String> getValues() {
        return values;
    }

    public Float getBias() {
        return bias;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Field) {
            Field field = (Field) obj;
            return this.name.equals(field.name)
                    && this.values.equals(field.values)
                    && this.bias.equals(field.bias);
        }
        return false;
    }
}
