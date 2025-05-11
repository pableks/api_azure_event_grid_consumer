package apiazureeventgridconsumer.model;

public class User {
    private Long id;
    private String email;
    private String password;
    private Long roleId;

    // Constructors
    public User() {}

    public User(Long id, String email, String password, Long roleId) {
        this.id = id;
        this.email = email;
        this.password = password;
        this.roleId = roleId;
    }

    // Getters and Setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public String getEmail() { return email; }
    public void setEmail(String email) { this.email = email; }

    public String getPassword() { return password; }
    public void setPassword(String password) { this.password = password; }
    
    public Long getRoleId() { return roleId; }
    public void setRoleId(Long roleId) { this.roleId = roleId; }
    
    @Override
    public String toString() {
        return "User{" +
                "id=" + id +
                ", email='" + email + '\'' +
                ", password='" + (password != null ? "****" : null) + '\'' +
                ", roleId=" + roleId +
                '}';
    }
}