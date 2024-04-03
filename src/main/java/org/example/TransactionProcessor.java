package org.example;

import lombok.Data;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TransactionProcessor {

    private static final Path USERS_CSV_PATH = Paths.get("/Users/olegtrofimov/IdeaProjects/Playtech Java Assignment 2024 1/test-data/test random data 50% validations/input/users.csv");
    private static final Path TRANSACTIONS_CSV_PATH = Paths.get("/Users/olegtrofimov/IdeaProjects/Playtech Java Assignment 2024 1/test-data/test random data 50% validations/input/transactions.csv");
    private static final Path BIN_MAPPINGS_CSV_PATH = Paths.get("/Users/olegtrofimov/IdeaProjects/Playtech Java Assignment 2024 1/test-data/test random data 50% validations/input/bins.csv");
    private static final Path BALANCES_CSV_PATH = Paths.get("/Users/olegtrofimov/IdeaProjects/Playtech Java Assignment 2024 1/test-data/test random data 50% validations/output example/balances.csv");
    private static final Path EVENTS_CSV_PATH = Paths.get("/Users/olegtrofimov/IdeaProjects/Playtech Java Assignment 2024 1/test-data/test random data 50% validations/output example/events.csv");

    public static void main(final String[] args) throws IOException {
        List<User> users = readUsers();
        List<Transaction> transactions = readTransactions();
        long startTime = System.currentTimeMillis();
        List<BinMapping> binMappings = readBinMappings();
        long endTime = System.currentTimeMillis();
        System.out.println("Execution time for readBinMappings(): " + (endTime - startTime) + " milliseconds");

        List<Event> events = processTransactions(users, transactions, binMappings);

        writeBalances(users);
        writeEvents(events);
    }

    private static List<User> readUsers() {
        List<User> users = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(TransactionProcessor.USERS_CSV_PATH.toFile()))) {
            String header = reader.readLine();
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length != 9) {
                    continue;
                }
                try {
                    String userId = parts[0];
                    String username = parts[1];
                    BigDecimal balance = new BigDecimal(parts[2]);
                    String country = parts[3];
                    int frozen = Integer.parseInt(parts[4]);
                    double depositMin = Double.parseDouble(parts[5]);
                    double depositMax = Double.parseDouble(parts[6]);
                    double withdrawMin = Double.parseDouble(parts[7]);
                    double withdrawMax = Double.parseDouble(parts[8]);

                    User user = new User(userId, username, balance, country, frozen, depositMin, depositMax, withdrawMin, withdrawMax);
                    users.add(user);
                } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return users;
    }

    private static List<Transaction> readTransactions() {
        List<Transaction> transactions = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(TransactionProcessor.TRANSACTIONS_CSV_PATH.toFile()))) {
            String header = reader.readLine();
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length != 6) {
                    continue;
                }
                try {
                    String transactionId = parts[0];
                    String userId = parts[1];
                    String type = parts[2];
                    BigDecimal amount = new BigDecimal(parts[3]);
                    String method = parts[4];
                    String accountNumber = parts[5];

                    Transaction transaction = new Transaction(transactionId, userId, type, amount, method, accountNumber);
                    transactions.add(transaction);
                } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return transactions;
    }

    private static List<BinMapping> readBinMappings() {
        List<BinMapping> binMappings = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(TransactionProcessor.BIN_MAPPINGS_CSV_PATH.toFile()))) {
            String header = reader.readLine();
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length != 5) {
                    continue;
                }
                String name = parts[0];
                long rangeFrom = Long.parseLong(parts[1]);
                long rangeTo = Long.parseLong(parts[2]);
                String type = parts[3];
                String country = parts[4];

                BinMapping binMapping = new BinMapping(name, rangeFrom, rangeTo, type, country);
                binMappings.add(binMapping);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return binMappings;
    }

    private static List<Event> processTransactions(final List<User> users,
                                                   final List<Transaction> transactions,
                                                   final List<BinMapping> binMappings) {
        List<Event> events = new ArrayList<>();
        Set<String> uniqueTransactionIds = new HashSet<>();

        for (Transaction transaction : transactions) {
            Event event = new Event();
            event.transactionId = transaction.getTransactionId();

            // Find if the user associated with the transaction
            User user = findUserById(users, transaction.getUserId());
            if (user == null) {
                event.status = Event.STATUS_DECLINED;
                event.message = "User not found";
                events.add(event);
                continue;
            }

            // Validate transaction ID uniqueness
            if (!uniqueTransactionIds.add(transaction.getTransactionId())) {
                event.status = Event.STATUS_DECLINED;
                event.message = "Transaction ID is not unique";
                events.add(event);
                continue;
            }

            // Find if the user is frozen
            if (user.getFrozen() == 1) {
                event.status = Event.STATUS_DECLINED;
                event.message = "User is frozen";
                events.add(event);
                continue;
            }

            //Validate payment method:
            if (!isValidPaymentMethod(transaction, binMappings, user.getCountry())) {
                event.status = Event.STATUS_DECLINED;
                event.message = "Invalid payment method";
                events.add(event);
                continue;
            }

            //Confirm that the country of the card or account used for the transaction matches the user's country
            if (!isTransactionCountryMatchingUserCountry(transaction, user.getCountry())) {
                event.status = Event.STATUS_DECLINED;
                event.message = "Country mismatch";
                events.add(event);
                continue;
            }

            // Validate that the amount is a valid (positive) number and within deposit/withdraw limits
            if (!isValidAmount(transaction, users)) {
                event.status = Event.STATUS_DECLINED;
                event.message = "Invalid amount";
                events.add(event);
                continue;
            }

            // For withdrawals, validate that the user has a sufficient balance
            if ("WITHDRAWAL".equals(transaction.getType()) && !hasSufficientBalance(user, transaction.getAmount())) {
                event.status = Event.STATUS_DECLINED;
                event.message = "Insufficient balance";
                events.add(event);
                continue;
            }

            // Allow withdrawals only with the same payment account that has previously been successfully used for deposit
            if ("WITHDRAWAL".equals(transaction.getType()) && !isWithdrawalAllowed(user, transaction, transactions)) {
                event.status = Event.STATUS_DECLINED;
                event.message = "Withdrawal not allowed";
                events.add(event);
                continue;
            }

            // Transaction type that isn't deposit or withdrawal should be declined
            if (!"DEPOSIT".equals(transaction.getType()) && !"WITHDRAWAL".equals(transaction.getType())) {
                event.status = Event.STATUS_DECLINED;
                event.message = "Invalid transaction type";
                events.add(event);
                continue;
            }

            // Users cannot share IBAN/card
            if (!isUniquePaymentAccount(transaction, users)) {
                event.status = Event.STATUS_DECLINED;
                event.message = "Payment account already used by another user";
                events.add(event);
                continue;
            }
            handleProcessingError(transaction, events, "Unexpected error occurred during transaction processing");
        }
        return events;
    }

    private static void handleProcessingError(Transaction transaction, List<Event> events, String errorMessage) {
        Event event = new Event();
        event.setTransactionId(transaction.getTransactionId());
        event.setStatus(Event.STATUS_DECLINED);
        event.setMessage(errorMessage);
        events.add(event);
    }

    private static boolean isUniquePaymentAccount(Transaction transaction, List<User> users) {
        String accountId = transaction.getAccountNumber();

        for (User user : users) {
            if (user.getUserId().equals(transaction.getUserId())) {
                continue;
            }

            if (hasUsedPaymentAccount(user, accountId)) {
                return false;
            }
        }
        return true;
    }

    private static boolean hasUsedPaymentAccount(User user, String accountId) {
        List<Transaction> allTransactions = getAllTransactions();
        for (Transaction userTransaction : allTransactions) {
            if (userTransaction.getAccountNumber().equals(accountId) && userTransaction.getUserId().equals(user.getUserId())) {
                return true;
            }
        }
        return false;
    }

    private static List<Transaction> getAllTransactions() {
        List<Transaction> transactions = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(TransactionProcessor.TRANSACTIONS_CSV_PATH.toFile()))) {
            reader.readLine();

            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length != 6) {
                    continue;
                }
                String transactionId = parts[0];
                String userId = parts[1];
                String type = parts[2];
                BigDecimal amount = new BigDecimal(parts[3]);
                String method = parts[4];
                String accountNumber = parts[5];

                Transaction transaction = new Transaction(transactionId, userId, type, amount, method, accountNumber);
                transactions.add(transaction);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return transactions;
    }

    private static boolean isWithdrawalAllowed(User user, Transaction transaction, List<Transaction> transactions) {
        if (!"WITHDRAW".equals(transaction.getType())) {
            return false;
        }

        List<Transaction> depositTransactions = getDepositTransactions(user.getUserId(), transactions);

        for (Transaction depositTransaction : depositTransactions) {
            if (depositTransaction.getMethod().equals(transaction.getMethod()) &&
                    depositTransaction.getAccountNumber().equals(transaction.getAccountNumber())) {
                return true;
            }
        }
        return false;
    }

    private static List<Transaction> getDepositTransactions(String userId, List<Transaction> transactions) {
        List<Transaction> depositTransactions = new ArrayList<>();
        for (Transaction transaction : transactions) {
            if (transaction.getUserId().equals(userId) && "DEPOSIT".equals(transaction.getType())) {
                depositTransactions.add(transaction);
            }
        }
        return depositTransactions;
    }

    private static boolean hasSufficientBalance(User user, BigDecimal amount) {
        return user.getBalance().compareTo(amount) >= 0;
    }


    private static boolean isValidAmount(Transaction transaction, List<User> users) {
        BigDecimal amount = transaction.getAmount();
        String userId = transaction.getUserId();
        String type = transaction.getType();

        if (type == null || type.isEmpty()) {
            return false;
        }

        User user = findUserById(users, userId);

        if (user == null) {
            return false;
        }

        if (amount.compareTo(BigDecimal.ZERO) <= 0) {
            return false;
        }

        if ("DEPOSIT".equalsIgnoreCase(type)) {
            return amount.compareTo(BigDecimal.valueOf(user.getDepositMin())) >= 0 &&
                    amount.compareTo(BigDecimal.valueOf(user.getDepositMax())) <= 0;
        } else if ("WITHDRAW".equalsIgnoreCase(type)) {
            return amount.compareTo(BigDecimal.valueOf(user.getWithdrawMin())) >= 0 &&
                    amount.compareTo(BigDecimal.valueOf(user.getWithdrawMax())) <= 0;
        }
        return false;
    }


    private static boolean isTransactionCountryMatchingUserCountry(Transaction transaction, String userCountry) {
        String transactionCountry = getTransactionCountry(transaction);
        return userCountry.equalsIgnoreCase(transactionCountry);
    }

    private static String getTransactionCountry(Transaction transaction) {
        String method = transaction.getMethod();
        String accountNumber = transaction.getAccountNumber();
        if ("TRANSFER".equals(method)) {
            return null;
        } else if ("CARD".equals(method)) {
            return getCardCountry(accountNumber);
        } else {
            return null;
        }
    }

    private static String getCardCountry(String accountNumber) {
        if (accountNumber.length() >= 2) {
            return accountNumber.substring(0, 2);
        } else {
            return null;
        }
    }

    private static boolean isValidPaymentMethod(Transaction transaction, List<BinMapping> binMappings, String userCountry) {
        String method = transaction.getMethod();

        switch (method) {
            case "TRANSFER":
                String accountNumber = transaction.getAccountNumber();
                return isValidIBAN(accountNumber);

            case "CARD":
                return isValidDebitCard(transaction, binMappings, userCountry);

            default:
                return false;
        }
    }

    private static boolean isValidDebitCard(Transaction transaction, List<BinMapping> binMappings, String userCountry) {
        String accountNumber = transaction.getAccountNumber();
        String cardPrefix = accountNumber.substring(0, 10);

        for (BinMapping binMapping : binMappings) {
            if (binMapping.getRangeFrom() <= Long.parseLong(cardPrefix)
                    && Long.parseLong(cardPrefix) <= binMapping.getRangeTo()) {
                if ("DC".equals(binMapping.getType()) && binMapping.getCountry().equals(userCountry)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static final int IBAN_MIN_SIZE = 15;
    private static final int IBAN_MAX_SIZE = 34;
    private static final long IBAN_MAX = 999999999;
    private static final long IBAN_MODULUS = 97;

    private static boolean isValidIBAN(String accountNumber) {
        String trimmed = accountNumber.trim();

        if (trimmed.length() < IBAN_MIN_SIZE || trimmed.length() > IBAN_MAX_SIZE) {
            return false;
        }

        String reformat = trimmed.substring(4) + trimmed.substring(0, 4);
        long total = 0;

        for (int i = 0; i < reformat.length(); i++) {
            char c = reformat.charAt(i);

            int charValue = Character.isDigit(c) ? Character.getNumericValue(c) : (c - 'A' + 10);

            if (charValue < 0 || charValue > 35) {
                return false;
            }

            total = (charValue > 9 ? total * 100 : total * 10) + charValue;

            if (total > IBAN_MAX) {
                total = (total % IBAN_MODULUS);
            }
        }
        return (total % IBAN_MODULUS) == 1;
    }


    private static User findUserById(List<User> users, String userId) {
        for (User user : users) {
            if (user.getUserId().equals(userId)) {
                return user;
            }
        }
        return null;
    }

    private static void writeBalances(final List<User> users) throws IOException {
        try (final FileWriter writer = new FileWriter(TransactionProcessor.BALANCES_CSV_PATH.toFile(), false)) {
            writer.append("user_id,balance\n");
            for (final var user : users) {
                writer.append(user.getUserId()).append(",")
                        .append(String.valueOf(user.getBalance())).append("\n");
            }
        }
    }

    private static void writeEvents(final List<Event> events) throws IOException {
        try (final FileWriter writer = new FileWriter(TransactionProcessor.EVENTS_CSV_PATH.toFile(), false)) {
            writer.append("transaction_id,status,message\n");
            for (final var event : events) {
                writer.append(event.transactionId).append(",").append(event.status)
                        .append(",").append(event.message).append("\n");
            }
        }
    }
}

@Data
class User {
    private String userId;
    private String username;
    private BigDecimal balance;
    private String country;
    private int frozen;
    private double depositMin;
    private double depositMax;
    private double withdrawMin;
    private double withdrawMax;

    public User(String userId, String username, BigDecimal balance, String country, int frozen,
                double depositMin, double depositMax, double withdrawMin, double withdrawMax) {
        this.userId = userId;
        this.username = username;
        this.balance = balance;
        this.country = country;
        this.frozen = frozen;
        this.depositMin = depositMin;
        this.depositMax = depositMax;
        this.withdrawMin = withdrawMin;
        this.withdrawMax = withdrawMax;
    }
}

@Data
class Transaction {
    private String transactionId;
    private String userId;
    private String type;
    private BigDecimal amount;
    private String method;
    private String accountNumber;

    public Transaction(String transactionId, String userId, String type,
                       BigDecimal amount, String method, String accountNumber) {
        this.transactionId = transactionId;
        this.userId = userId;
        this.type = type;
        this.amount = amount;
        this.method = method;
        this.accountNumber = accountNumber;
    }
}

@Data
class BinMapping {
    private String name;
    private long rangeFrom;
    private long rangeTo;
    private String type;
    private String country;

    public BinMapping(String name, long rangeFrom, long rangeTo, String type, String country) {
        this.name = name;
        this.rangeFrom = rangeFrom;
        this.rangeTo = rangeTo;
        this.type = type;
        this.country = country;
    }
}

@Data
class Event {
    public static final String STATUS_DECLINED = "DECLINED";
    public static final String STATUS_APPROVED = "APPROVED";

    public String transactionId;
    public String status;
    public String message;
}
