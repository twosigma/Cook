package com.twosigma.cook.jobclient.constraint;

import com.google.common.base.Preconditions;
import com.twosigma.cook.jobclient.constraint.api.Constraint;
import com.twosigma.cook.jobclient.constraint.api.ConstraintBuilder;
import com.twosigma.cook.jobclient.constraint.api.Operator;
import org.json.JSONArray;

public final class OneToOneConstraintBuilder implements ConstraintBuilder {
    private Operator _operator;

    OneToOneConstraintBuilder(Operator operator) {
        _operator = operator;
    }

    @Override
    public Operator getOperator() {
        return _operator;
    }

    @Override
    public Constraint build(String attribute, Object... values) {
        Preconditions.checkArgument(values.length == 1,
                "Operator " + _operator + " could only be applied to a single value.");
        return new OneToOneConstraint(_operator, attribute, values[0].toString());
    }

    @Override
    public Constraint parseFrom(JSONArray constraint) {
        Preconditions.checkArgument(constraint.length() == 3,
                "The constraint must be form of [<attribute>, <operator>, <value>]");
        Operator operator = Operator.fromString(constraint.getString(1));
        Preconditions.checkArgument(operator == _operator,
                "Can't parse the given constraint as its operator doesn't match.");
        return new OneToOneConstraint(_operator, constraint.getString(0), constraint.getString(2));
    }

    static class OneToOneConstraint implements Constraint {
        private Operator _operator;
        private String _attribute;
        private String _value;

        private OneToOneConstraint(Operator operator, String attribute, String value) {
            _operator = operator;
            _attribute = attribute;
            _value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            OneToOneConstraint that = (OneToOneConstraint) o;

            if (_attribute != null ? !_attribute.equals(that._attribute) : that._attribute != null) {
                return false;
            }
            if (_operator != that._operator) {
                return false;
            }
            return _value != null ? _value.equals(that._value) : that._value == null;
        }

        @Override
        public int hashCode() {
            int result = _attribute != null ? _attribute.hashCode() : 0;
            result = 31 * result + (_operator != null ? _operator.hashCode() : 0);
            result = 31 * result + (_value != null ? _value.hashCode() : 0);
            return result;
        }


        /**
         * @return this constraint as a JSONArray.
         */
        @Override
        public JSONArray toJson() {
            JSONArray jsonArray = new JSONArray();
            jsonArray.put(0, _attribute);
            jsonArray.put(1, _operator.toString());
            jsonArray.put(2, _value);
            return jsonArray;
        }

        @Override
        public String getAttribute() {
            return _attribute;
        }

        @Override
        public Operator getOperator() {
            return _operator;
        }

        @Override
        public Object getValue() {
            return _value;
        }
    }
}
