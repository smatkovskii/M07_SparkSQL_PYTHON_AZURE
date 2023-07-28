#!/bin/bash
trap 'echo interrupted; exit' INT
set -e


echo "Preparing to create Azure infrastructure..."

read -p "Azure region in which all resources will be created (default: westeurope): " azureLocation
if ! [ $azureLocation ] ; then
    azureLocation=westeurope
fi

read -p "The prefix for all resources in the environment. Please use letters and digits only: " envPrefix
if ! [[ "$envPrefix" =~ ^[0-9A-Za-z]+$ ]] ; then 
    exec >&2; echo "Value '$envPrefix' is not valid"; exit 1
fi

azureAccountId=`az account show --query "id" || true`
if [ $azureAccountId ] ; then
    echo "This script will run on behalf of Azure Account ID $azureAccountId"
else
    echo "Seems like the user is not signed into Azure. Initiating login procedure..."
    az login
fi

TF_RES_GROUP_NAME="$envPrefix"_rg_tfstate
TF_STORAGE_ACC_NAME="$envPrefix"tfstate"$RANDOM"
TF_CONTAINER_NAME=tfstate

if [ `az group exists --name $TF_RES_GROUP_NAME` = true ]; then
    echo "Resource group '$TF_RES_GROUP_NAME' exists. Checking the storage account..."
    TF_STORAGE_ACC_NAME=`az storage account list -g $TF_RES_GROUP_NAME --query "[0].name" | tr -d '"'`
    if [ $TF_STORAGE_ACC_NAME ] ; then
        echo "Found storage account '$TF_STORAGE_ACC_NAME'. Continuing..."
    else
        echo "Resource group '$TF_RES_GROUP_NAME' does not contain storage accounts. Aborting"
        exit 1
    fi
else 
    echo "Creating a resource group for Terraform state..."
    az group create --name $TF_RES_GROUP_NAME --location $azureLocation

    echo "Creating a storage account for Terraform state..."
    az storage account create --resource-group $TF_RES_GROUP_NAME --name $TF_STORAGE_ACC_NAME \
        --sku Standard_LRS --encryption-services blob

    echo "Creating a blob container for Terraform state..."
    az storage container create --name $TF_CONTAINER_NAME --account-name $TF_STORAGE_ACC_NAME

    echo "Storage for Terraform state created."
fi

echo "Initializing Terraform..."
terraform -chdir=terraform init \
    -backend-config="resource_group_name=$TF_RES_GROUP_NAME" \
    -backend-config="storage_account_name=$TF_STORAGE_ACC_NAME" \
    -backend-config="container_name=$TF_CONTAINER_NAME" \
    -backend-config="key=terraform.tfstate"

echo "Creating and applying Terraform plan..."
terraform -chdir=terraform apply -var "ENV=$envPrefix" -var "LOCATION=$azureLocation"

echo "\n\nInfrastructure has been created."
