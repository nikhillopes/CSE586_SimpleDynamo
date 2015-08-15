package edu.buffalo.cse.cse486586.simpledynamo;

public enum MessageType {
    INSERT {
        @Override
        public String toString() {
            return "INSERT";
        }
    },
    INSERT_REPLICA_1 {
        @Override
        public String toString() {
            return "INSERT_REPLICA_1";
        }
    },
    INSERT_REPLICA_2 {
        @Override
        public String toString() {
            return "INSERT_REPLICA_2";
        }
    },
    DELETE {
        @Override
        public String toString() {
            return "DELETE";
        }
    },
    DELETE_REPLICA_1 {
        @Override
        public String toString() {
            return "DELETE_REPLICA_1";
        }
    },
    DELETE_REPLICA_2 {
        @Override
        public String toString() {
            return "DELETE_REPLICA_2";
        }
    },
    INSERT_DELETE_REPLY {
        @Override
        public String toString() {
            return "INSERT_DELETE_REPLY";
        }
    },
    QUERY {
        @Override
        public String toString() {
            return "QUERY";
        }
    },
    QUERY_REPLICA_1 {
        @Override
        public String toString() {
            return "QUERY_REPLICA_1";
        }
    },
    QUERY_REPLICA_2 {
        @Override
        public String toString() {
            return "QUERY_REPLICA_2";
        }
    },
    QUERY_REPLY {
        @Override
        public String toString() {
            return "QUERY_REPLY";
        }
    };

    public int getIntVal(MessageType findVal) {
        if (findVal == INSERT_DELETE_REPLY) {
            return 1;
        } else if (findVal == QUERY_REPLY) {
            return 2;
        } else if (findVal == INSERT) {
            return 3;
        } else if (findVal == INSERT_REPLICA_1) {
            return 4;
        } else if (findVal == INSERT_REPLICA_2) {
            return 5;
        } else if (findVal == QUERY) {
            return 6;
        } else if (findVal == QUERY_REPLICA_1) {
            return 8;
        } else if (findVal == QUERY_REPLICA_2) {
            return 10;
        } else if (findVal == DELETE) {
            return 12;
        } else if (findVal == DELETE_REPLICA_1) {
            return 13;
        } else if (findVal == DELETE_REPLICA_2) {
            return 14;
        }
        return -1;
    }

    public static MessageType getMessageType(int findVal) {
        if (findVal == 1) {
            return INSERT_DELETE_REPLY;
        } else if (findVal == 2) {
            return QUERY_REPLY;
        } else if (findVal == 3) {
            return INSERT;
        } else if (findVal == 4) {
            return INSERT_REPLICA_1;
        } else if (findVal == 5) {
            return INSERT_REPLICA_2;
        } else if (findVal == 6) {
            return QUERY;
        } else if (findVal == 8) {
            return QUERY_REPLICA_1;
        } else if (findVal == 10) {
            return QUERY_REPLICA_2;
        } else if (findVal == 12) {
            return DELETE;
        } else if (findVal == 13) {
            return DELETE_REPLICA_1;
        } else if (findVal == 14) {
            return DELETE_REPLICA_2;
        }
        return null;
    }
}