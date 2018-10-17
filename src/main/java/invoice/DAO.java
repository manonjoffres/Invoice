package invoice;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.DataSource;

public class DAO {

	private final DataSource myDataSource;

	/**
	 *
	 * @param dataSource la source de données à utiliser
	 */
	public DAO(DataSource dataSource) {
		this.myDataSource = dataSource;
	}

	/**
	 * Renvoie le chiffre d'affaire d'un client (somme du montant de ses factures)
	 *
	 * @param id la clé du client à chercher
	 * @return le chiffre d'affaire de ce client ou 0 si pas trouvé
	 * @throws SQLException
	 */
	public float totalForCustomer(int id) throws SQLException {
		String sql = "SELECT SUM(Total) AS Amount FROM Invoice WHERE CustomerID = ?";
		float result = 0;
		try (Connection connection = myDataSource.getConnection();
			PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setInt(1, id); // On fixe le 1° paramètre de la requête
			try (ResultSet resultSet = statement.executeQuery()) {
				if (resultSet.next()) {
					result = resultSet.getFloat("Amount");
				}
			}
		}
		return result;
	}

	/**
	 * Renvoie le nom d'un client à partir de son ID
	 *
	 * @param id la clé du client à chercher
	 * @return le nom du client (LastName) ou null si pas trouvé
	 * @throws SQLException
	 */
	public String nameOfCustomer(int id) throws SQLException {
		String sql = "SELECT LastName FROM Customer WHERE ID = ?";
		String result = null;
		try (Connection connection = myDataSource.getConnection();
			PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setInt(1, id);
			try (ResultSet resultSet = statement.executeQuery()) {
				if (resultSet.next()) {
					result = resultSet.getString("LastName");
				}
			}
		}
		return result;
	}

	/**
	 * Transaction permettant de créer une facture pour un client
	 *
	 * @param customer Le client
	 * @param productIDs tableau des numéros de produits à créer dans la facture
	 * @param quantities tableau des quantités de produits à facturer faux sinon Les deux tableaux doivent avoir la même
	 * taille
	 * @throws java.lang.Exception si la transaction a échoué
	 */
	public void createInvoice(CustomerEntity customer, int[] productIDs, int[] quantities) throws SQLException, Exception {  
            String sql1 = "INSERT INTO Invoice(CustomerID) VALUES (?) ";  /* ID => clé générée auto
                                                                            * Le trigger mettra à jour le total automatiquement          
                                                                            */
            String sql2 = "INSERT INTO Item(InvoiceID, Item, ProductID, Quantity, Cost) VALUES (?,?,?,?,?) ";
            String sql3 = "SELECT Price AS PRIX FROM Product WHERE ID = ?";
            
             try (Connection myConnection = myDataSource.getConnection();
                    // On prépare la requête en précisant qu'on veut récupérer les clés auto-générées
                    PreparedStatement statement1 = myConnection.prepareStatement(sql1, Statement.RETURN_GENERATED_KEYS);  // On a besoin de la clé primaire de l'Invoice pour créer un Item
                    PreparedStatement statement2 = myConnection.prepareStatement(sql2);
                    PreparedStatement statement3 = myConnection.prepareStatement(sql3)) {           
            
                myConnection.setAutoCommit(false); // On démarre une transaction
                try {
                    
                    /**----------------------------------------------------------------------------------**/
                    // Table Invoice
                    statement1.setInt(1, customer.getCustomerId());
                        
                     // On exécute la requête, la clé est auto-générée à ce moment là
                    int numberUpdated1 = statement1.executeUpdate();
                    
                    if (numberUpdated1 != 1) {  // On n'a pas trouvé                        
                        throw new Exception("Les valeurs n'ont pas été insérées ");
                    }
                    
                    // Les clefs autogénérées sont retournées sous forme de ResultSet, 
                    // car il se peut qu'une requête génère plusieurs clés
                    ResultSet clefs = statement1.getGeneratedKeys(); 

                    clefs.next(); // On lit la première clé générée
                    int invoiceID = clefs.getInt(1);
                    System.out.println("La première clef autogénérée vaut " + invoiceID);
                   

                    /**----------------------------------------------------------------------------------**/
                    
                    // Table Item (InvoiceID, Item, ProductID, Quantity, Cost)
                    // Définir les paramètres éventuels de la requête                  
                    for(int i = 0 ; i < productIDs.length ; i++) {
                        int idProduit = productIDs[i];
                        
                        statement2.setInt(1, invoiceID);
                        statement2.setInt(2, i);  // clé primaire Item
                        statement2.setInt(3, idProduit);
                        statement2.setInt(4, quantities[i]);
                        
                        /**----------------------------------------------------------------------------------**/
                        /* Récupération du prix du produit en question */
                        statement3.setInt(1, idProduit);
                        float prix = 0f;
                        
                        try (ResultSet resultSet = statement3.executeQuery()) {
				if (resultSet.next()) {
                                    prix = resultSet.getFloat("PRIX");
				}
			}
                        /**----------------------------------------------------------------------------------**/
                        
                        statement2.setFloat(5, prix);
                     
                        // On exécute la requête, la clé est auto-générée à ce moment là
                        int numberUpdated2 = statement2.executeUpdate();
                        if (numberUpdated2 != 1) // On n'a pas trouvé
                        {
                            throw new Exception("Les valeurs n'ont pas été insérées ");
                        }
                    }
              
                /**----------------------------------------------------------------------------------**/
                // Tout s'est bien passé, on peut valider la transaction
                    myConnection.commit();
                } catch (Exception ex) {
                    myConnection.rollback(); // On annule la transaction
                    throw ex;
                } finally { // On revient au mode de fonctionnement sans transaction
                    myConnection.setAutoCommit(true);
                }
            }
           

            
	}

	/**
	 *
	 * @return le nombre d'enregistrements dans la table CUSTOMER
	 * @throws SQLException
	 */
	public int numberOfCustomers() throws SQLException {
		int result = 0;

		String sql = "SELECT COUNT(*) AS NUMBER FROM Customer";
		try (Connection connection = myDataSource.getConnection();
			Statement stmt = connection.createStatement()) {
			ResultSet rs = stmt.executeQuery(sql);
			if (rs.next()) {
				result = rs.getInt("NUMBER");
			}
		}
		return result;
	}

	/**
	 *
	 * @param customerId la clé du client à recherche
	 * @return le nombre de bons de commande pour ce client (table PURCHASE_ORDER)
	 * @throws SQLException
	 */
	public int numberOfInvoicesForCustomer(int customerId) throws SQLException {
		int result = 0;

		String sql = "SELECT COUNT(*) AS NUMBER FROM Invoice WHERE CustomerID = ?";

		try (Connection connection = myDataSource.getConnection();
			PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setInt(1, customerId);
			ResultSet rs = stmt.executeQuery();
			if (rs.next()) {
				result = rs.getInt("NUMBER");
			}
		}
		return result;
	}

	/**
	 * Trouver un Customer à partir de sa clé
	 *
	 * @param customedID la clé du CUSTOMER à rechercher
	 * @return l'enregistrement correspondant dans la table CUSTOMER, ou null si pas trouvé
	 * @throws SQLException
	 */
	CustomerEntity findCustomer(int customerID) throws SQLException {
		CustomerEntity result = null;

		String sql = "SELECT * FROM Customer WHERE ID = ?";
		try (Connection connection = myDataSource.getConnection();
			PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setInt(1, customerID);

			ResultSet rs = stmt.executeQuery();
			if (rs.next()) {
				String name = rs.getString("FirstName");
				String address = rs.getString("Street");
				result = new CustomerEntity(customerID, name, address);
			}
		}
		return result;
	}

	/**
	 * Liste des clients localisés dans un état des USA
	 *
	 * @param state l'état à rechercher (2 caractères)
	 * @return la liste des clients habitant dans cet état
	 * @throws SQLException
	 */
	List<CustomerEntity> customersInCity(String city) throws SQLException {
		List<CustomerEntity> result = new LinkedList<>();

		String sql = "SELECT * FROM Customer WHERE City = ?";
		try (Connection connection = myDataSource.getConnection();
			PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setString(1, city);
			try (ResultSet rs = stmt.executeQuery()) {
				while (rs.next()) {
					int id = rs.getInt("ID");
					String name = rs.getString("FirstName");
					String address = rs.getString("Street");
					CustomerEntity c = new CustomerEntity(id, name, address);
					result.add(c);
				}
			}
		}

		return result;
	}
}
