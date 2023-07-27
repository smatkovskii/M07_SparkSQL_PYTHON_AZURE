# Setup azurerm as a state backend
terraform {
  backend "azurerm" {
    # TODO move to script arguments
    resource_group_name = "sm0723_rg_tfstate"
    storage_account_name = "sm0723tfstate"
    container_name = "tfstate"
    key = "terraform.tfstate"
  }
}

# Configure the Microsoft Azure Provider
provider "azurerm" {
  features {}
}

data "azurerm_client_config" "current" {}

resource "azurerm_resource_group" "bdcc" {
  name = "rg-${var.ENV}-${var.LOCATION}"
  location = var.LOCATION

  tags = {
    region = var.BDCC_REGION
    env = var.ENV
  }
}

resource "azurerm_storage_account" "bdcc" {
  depends_on = [
    azurerm_resource_group.bdcc]

  name = "stor${var.ENV}${var.LOCATION}"
  resource_group_name = azurerm_resource_group.bdcc.name
  location = azurerm_resource_group.bdcc.location
  account_tier = "Standard"
  account_replication_type = var.STORAGE_ACCOUNT_REPLICATION_TYPE
  is_hns_enabled = "true"

  tags = {
    region = var.BDCC_REGION
    env = var.ENV
  }
}

resource "azurerm_role_assignment" "role_assignment" {
  depends_on = [
    azurerm_storage_account.bdcc]

  scope                = azurerm_storage_account.bdcc.id
  role_definition_name = "Storage Blob Data Owner"
  principal_id         = data.azurerm_client_config.current.object_id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "gen2_data" {
  depends_on = [
    azurerm_role_assignment.role_assignment]

  name = "data"
  storage_account_id = azurerm_storage_account.bdcc.id
}

resource "azurerm_databricks_workspace" "bdcc" {
  depends_on = [
    azurerm_resource_group.bdcc
  ]

  name = "dbw-${var.ENV}-${var.LOCATION}"
  resource_group_name = azurerm_resource_group.bdcc.name
  location = azurerm_resource_group.bdcc.location
  sku = "standard"

  tags = {
    region = var.BDCC_REGION
    env = var.ENV
  }
}
