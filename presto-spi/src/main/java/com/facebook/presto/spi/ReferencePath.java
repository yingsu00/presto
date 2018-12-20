/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.spi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import static java.util.Objects.requireNonNull;


public class ReferencePath
{

    public static class PathElement
    {
        private final String field;
        private final int subscript;

        @JsonCreator
        public PathElement(
                           @JsonProperty("Dfieldid")String field,
                           @JsonProperty("subscript") int subscript)
        {
            this.field = field;
            this.subscript = subscript;
        }

        @JsonProperty("field")
        public String getField()
        {
            return field;
        }

        @JsonProperty("subscript")
        public int getSubscript()
        {
            return subscript;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PathElement other = (PathElement) o;
            if (field == null && other.field == null) {
                return subscript == other.subscript;
            }
            if (field == null || other.field == null) {
                return false;
            }
            return field.equals(other.field);
        }

        @Override
        public int hashCode()
        {
            return field != null ? field.hashCode() : subscript;
        }


        @Override
        public String toString()
        {
            if (field != null) {
                return field;
            }
            return new Integer(subscript).toString();
        }
    }

    private final ArrayList<PathElement> path;

    @JsonCreator
    public ReferencePath(
                         @JsonProperty("path") ArrayList<PathElement> path)
    {
        requireNonNull(path, "path is null");
        this.path = path;
    }

    @JsonProperty("path")
    public ArrayList<PathElement> getPath()
    {
        return path;
    }

    @Override
    public String toString()
    {
        String result = "";
        for (int i = 0; i < path.size(); i++) {
            result = result + path.get(i).toString() + (i < path.size() - 1 ? "." : "");
                }
        return result;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ReferencePath otherPath = (ReferencePath) o;

        if (otherPath.path.size() != path.size()) {
            return false;
        }
        for (int i = 0; i < path.size(); i++) {
            if (!path.get(i).equals(otherPath.path.get(i))) {
                return false;
            }
            }

        return true;
    }
    
    @Override
    public int hashCode()
    {
        int hashCode = 0;
        for (PathElement element : path) {
            hashCode = hashCode + hashCode * element.hashCode();
        }
        return hashCode;
    }

}
